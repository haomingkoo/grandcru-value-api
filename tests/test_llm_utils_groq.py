import io
import json
import unittest
from unittest import mock
from urllib.error import HTTPError

from scripts import llm_utils


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def _completion(text: str) -> _Response:
    return _Response(json.dumps({"choices": [{"message": {"content": text}}]}).encode("utf-8"))


def _http_error(code: int, retry_after: str | None = None) -> HTTPError:
    headers = {"retry-after": retry_after} if retry_after else {}
    return HTTPError("https://api.groq.com", code, "error", headers, io.BytesIO(b"{}"))


class CallLlmTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch.object(llm_utils.time, "sleep")
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)

    def test_sends_prompt_to_groq_and_returns_the_reply(self) -> None:
        with mock.patch.object(llm_utils, "urlopen", return_value=_completion('{"decision": "match"}')) as urlopen:
            reply = llm_utils.call_llm("Match this wine", "test-key")

        self.assertEqual(reply, '{"decision": "match"}')
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://api.groq.com/openai/v1/chat/completions")
        self.assertEqual(request.get_header("Authorization"), "Bearer test-key")
        body = json.loads(request.data)
        self.assertEqual(body["model"], llm_utils.GROQ_MODEL)
        self.assertEqual(body["messages"], [{"role": "user", "content": "Match this wine"}])

    def test_waits_and_retries_when_rate_limited(self) -> None:
        outcomes = [_http_error(429, retry_after="7"), _completion("ok")]
        with mock.patch.object(llm_utils, "urlopen", side_effect=outcomes) as urlopen:
            reply = llm_utils.call_llm("prompt", "test-key")

        self.assertEqual(reply, "ok")
        self.assertEqual(urlopen.call_count, 2)
        self.sleep.assert_called_once_with(7.0)

    def test_raises_after_repeated_rate_limits(self) -> None:
        with mock.patch.object(llm_utils, "urlopen", side_effect=[_http_error(429)] * 10) as urlopen:
            with self.assertRaises(HTTPError):
                llm_utils.call_llm("prompt", "test-key")

        self.assertEqual(urlopen.call_count, llm_utils.LLM_REQUEST_ATTEMPTS)

    def test_does_not_retry_a_rejected_key(self) -> None:
        with mock.patch.object(llm_utils, "urlopen", side_effect=[_http_error(401), _completion("ok")]) as urlopen:
            with self.assertRaises(HTTPError):
                llm_utils.call_llm("prompt", "bad-key")

        self.assertEqual(urlopen.call_count, 1)


if __name__ == "__main__":
    unittest.main()
