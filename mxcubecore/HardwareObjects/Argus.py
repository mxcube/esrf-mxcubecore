import grpc
import gevent
from google.protobuf.json_format import MessageToDict
from typing import List

import argussight.grpc.argus_service_pb2 as pb2
import argussight.grpc.argus_service_pb2_grpc as pb2_grpc

from mxcubecore.BaseHardwareObjects import HardwareObject


class Argus(HardwareObject):
    def __init__(self, name):
        super().__init__(name)
        channel = grpc.insecure_channel("localhost:50051")
        self.stub = pb2_grpc.SpawnerServiceStub(channel)
        self.running_processes = {}
        self.available_classes = {}
        self.last_response = {}
        self.streams = {}  # save all available camerastreamlocations here
        self.server_communication_error = False
        self.closable_running = (
            False  # keep track if there are any closable processes are running
        )
        gevent.spawn(self.emit_process_change)

    def init(self):
        super().init()

    def get_processes_from_server(self) -> dict:
        try:
            response = self.stub.GetProcesses(pb2.GetProcessesRequest())
            if response.status == "success":
                if self.last_response == {} or self.last_response["status"] == "error":
                    self.last_response = {}
                response_dict = MessageToDict(response)
                return (
                    response_dict["runningProcesses"],
                    response_dict["availableProcessTypes"],
                )
        except Exception as e:
            self.server_communication_error = True
            self.emit_last_response_change("error", str(e))
            return {"Error": {"state": "UNKNOWN", "type": "Server-Connection"}}, {}

    def emit_process_change(self):
        while True:
            current_running, classes = self.get_processes_from_server()
            if (
                current_running != self.running_processes
                or classes != self.available_classes
            ):
                self.running_processes = current_running
                self.available_classes = classes

                # check if any streaming processes have terminated
                streams_to_remove = [
                    streams
                    for streams in self.streams.keys()
                    if streams not in current_running.keys()
                ]
                self.remove_streams(streams_to_remove)

                # check if any process started by user is running
                self.closable_running = False
                for process in current_running:
                    if current_running[process]["type"] in classes:
                        self.closable_running = True
                        break

                self.emit("processesChanged")
                self.emit("lastResponseChanged")
            gevent.sleep(2)

    def get_processes(self) -> dict:
        return {
            "running": self.running_processes,
            "available": self.available_classes,
            "closable_running": self.closable_running,
        }

    def get_last_response(self) -> dict:
        return self.last_response

    def stop_process(self, name: str):
        print(f"Sending termination request for {name}")
        response = self.stub.TerminateProcesses(
            pb2.TerminateProcessesRequest(names=[name])
        )
        self.emit_last_response_change(response.status, response.error_message)

    def start_process(self, name: str, type: str, **args):
        print(f"Sending start process request for {name}")
        process = pb2.ProcessInfo(
            name=name,
            type=type,
            args=[arg for arg in args["args"]],
        )
        response = self.stub.StartProcesses(pb2.StartProcessesRequest(process=process))
        self.emit_last_response_change(response.status, response.error_message)
        if response.stream_id:
            self.new_stream(name, response.stream_id)

    def manage_process(self, name: str, command: str, wait_time: int, **args):
        print(f"Sending manage request for command {command} of process {name}")
        request = pb2.ManageProcessesRequest(
            name=name,
            order=command,
            wait_time=wait_time,
            args=[arg for arg in args["args"]],
        )
        response = self.stub.ManageProcesses(request)
        self.emit_last_response_change(response.status, response.error_message)

    def emit_last_response_change(self, status, error_message):
        self.last_response = {
            "status": status,
            "error_message": error_message,
        }
        self.emit("lastResponseChanged")

    def new_stream(self, name: str, stream_id: str):
        self.streams[name] = stream_id
        self.emit("streamsChanged")

    def remove_streams(self, streams: List[str]):
        for stream in streams:
            del self.streams[stream]
        self.emit("streamsChanged")

    def get_streams(self):
        return self.streams
