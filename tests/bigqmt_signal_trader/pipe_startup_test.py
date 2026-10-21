"""Pipe initialization must finish before QMT starts background listeners."""
import unittest
from unittest.mock import patch

from bigqmt_signal_trader.transports import pipe_push_channel, pipe_transport


class PipeStartupTest(unittest.TestCase):
    def test_publisher_initializes_once_before_thread_start(self):
        channel = pipe_push_channel.PipeQuotePushChannel(bind_address="test")
        kernel = (object(), object())
        events = []

        def initialize():
            events.append("initialize")
            return kernel

        with patch.object(pipe_push_channel, "_kernel32", side_effect=initialize) as init:
            with patch.object(pipe_push_channel.threading, "Thread") as thread:
                thread.return_value.start.side_effect = lambda: events.append("start")
                channel.start_publisher()
                self.assertIs(channel._dll(), kernel)
                self.assertIs(channel._dll(), kernel)
        self.assertEqual(events, ["initialize", "start"])
        init.assert_called_once_with()

    def test_initialization_failure_does_not_start_or_mark_running(self):
        publisher = pipe_push_channel.PipeQuotePushChannel(bind_address="test")
        subscriber = pipe_push_channel.PipeQuotePushChannel(connect_address="test")
        rpc = pipe_transport.NamedPipeTransport()
        cases = [
            (publisher, lambda: publisher.start_publisher()),
            (subscriber, lambda: subscriber.start_subscriber([], lambda *args: None)),
            (rpc, lambda: rpc.start_receiving(lambda request: None)),
        ]
        for instance, start in cases:
            with self.subTest(instance=type(instance).__name__):
                with patch.object(instance, "_dll", side_effect=AttributeError("HANDLE")):
                    with patch.object(pipe_push_channel.threading, "Thread") as thread:
                        with self.assertRaisesRegex(AttributeError, "HANDLE"):
                            start()
                        thread.assert_not_called()
                self.assertFalse(instance._running)

    def test_rpc_initializes_before_listener_start(self):
        rpc = pipe_transport.NamedPipeTransport()
        events = []
        with patch.object(rpc, "_dll", side_effect=lambda: events.append("initialize")):
            with patch.object(pipe_transport.threading, "Thread") as thread:
                thread.return_value.start.side_effect = lambda: events.append("start")
                rpc.start_receiving(lambda request: None)
        self.assertEqual(events, ["initialize", "start"])


if __name__ == "__main__":
    unittest.main()
