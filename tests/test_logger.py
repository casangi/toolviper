import pytest
import os
import logging
from unittest.mock import MagicMock, patch
from toolviper.utils.logger import (
    set_verbosity,
    verbosity,
    info,
    debug,
    warning,
    error,
    critical,
    exception,
    log,
    get_logger,
    setup_logger,
    ColorLoggingFormatter,
    LoggingFormatter,
    get_worker_logger_name,
    setup_worker_logger,
)


@pytest.fixture(autouse=True)
def reset_verbosity():
    token = verbosity.set(None)
    yield
    verbosity.reset(token)


@pytest.fixture
def mock_logger():
    with patch("toolviper.utils.logger.get_logger") as mock:
        logger = MagicMock(spec=logging.Logger)
        mock.return_value = logger
        yield logger


def test_set_verbosity():
    set_verbosity(True)
    assert verbosity.get() is True
    set_verbosity(False)
    assert verbosity.get() is False
    set_verbosity(None)
    assert verbosity.get() is None


def test_info_logging(mock_logger):
    info("test message")
    mock_logger.info.assert_called_with("test message")


def test_info_verbose_logging(mock_logger):
    with patch(
        "toolviper.utils.logger.add_verbose_info", return_value="verbose test"
    ) as mock_add:
        info("test message", verbose=True)
        mock_add.assert_called()
        mock_logger.info.assert_called_with("verbose test")


def test_info_verbosity_context(mock_logger):
    set_verbosity(True)
    with patch("toolviper.utils.logger.add_verbose_info", return_value="verbose test"):
        info("test message")
        mock_logger.info.assert_called_with("verbose test")


def test_debug_logging(mock_logger):
    debug("debug message")
    mock_logger.debug.assert_called_with("debug message")


def test_warning_logging(mock_logger):
    warning("warning message")
    mock_logger.warning.assert_called_with("warning message")


def test_error_logging(mock_logger):
    with patch(
        "toolviper.utils.logger.add_verbose_info",
        side_effect=lambda message, color: message,
    ):
        error("error message")
    mock_logger.error.assert_called_with("error message")


def test_critical_logging(mock_logger):
    with patch(
        "toolviper.utils.logger.add_verbose_info",
        side_effect=lambda message, color: message,
    ):
        critical("critical message")
    mock_logger.critical.assert_called_with("critical message")


def test_exception_logging(mock_logger):
    exception("exception message")
    mock_logger.exception.assert_called_with("exception message")


def test_log_logging(mock_logger):
    mock_logger.level = logging.INFO
    log("log message")
    mock_logger.log.assert_called_with(logging.INFO, "log message")


def test_get_logger_no_env_no_worker(monkeypatch):
    monkeypatch.delenv("LOGGER_NAME", raising=False)
    with patch("toolviper.utils.logger.get_worker", side_effect=ValueError):
        logger = get_logger()
        assert logger.name == "viperlog"
        # Since it's a new logger, it should have a StreamHandler
        assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


def test_get_logger_existing_logger(monkeypatch):
    monkeypatch.delenv("LOGGER_NAME", raising=False)
    # Pre-create logger
    existing_logger = logging.getLogger("existing_log")
    with patch("toolviper.utils.logger.get_worker", side_effect=ValueError):
        logger = get_logger("existing_log")
        assert logger == existing_logger


def test_get_logger_env():
    with (
        patch("os.environ", {"LOGGER_NAME": "env_logger"}),
        patch("toolviper.utils.logger.get_worker", side_effect=ValueError),
    ):
        logger = get_logger()
        assert logger.name == "env_logger"


def test_get_logger_worker():
    mock_worker = MagicMock()
    mock_logger_obj = MagicMock()
    mock_worker.plugins = {"worker_logger": MagicMock()}
    mock_worker.plugins["worker_logger"].get_logger.return_value = mock_logger_obj

    with patch("toolviper.utils.logger.get_worker", return_value=mock_worker):
        logger = get_logger("test_logger")
        assert logger == mock_logger_obj


def test_setup_logger_basic(tmp_path):
    log_file_base = str(tmp_path / "test_log")
    logger = setup_logger(
        logger_name="setup_test",
        log_to_term=True,
        log_to_file=True,
        log_file=log_file_base,
    )
    assert logger.name == "setup_test"
    assert len(logger.handlers) == 2
    # Cleanup
    for handler in logger.handlers:
        handler.close()


def test_color_logging_formatter():
    formatter = ColorLoggingFormatter()
    record = logging.LogRecord("name", logging.INFO, "path", 10, "msg", None, None)
    formatted = formatter.format(record)
    assert "INFO" in formatted
    assert "msg" in formatted


def test_logging_formatter():
    formatter = LoggingFormatter()
    record = logging.LogRecord(
        "name", logging.ERROR, "path", 20, "error msg", None, None
    )
    formatted = formatter.format(record)
    assert "ERROR" in formatted
    assert "error msg" in formatted


def test_get_worker_logger_name():
    mock_worker = MagicMock()
    mock_worker.id = "worker-123"
    with patch("toolviper.utils.logger.get_worker", return_value=mock_worker):
        name = get_worker_logger_name("mylog")
        assert name == "mylog_worker-123"


def test_setup_worker_logger(tmp_path):
    mock_worker = MagicMock()
    mock_worker.name = "worker-1"
    mock_worker.ip = "127.0.0.1"
    log_file_base = str(tmp_path / "worker_log")

    with patch("dask.distributed.print"):
        logger = setup_worker_logger(
            logger_name="worker_test",
            log_to_term=True,
            log_to_file=True,
            log_file=log_file_base,
            log_level="DEBUG",
            worker=mock_worker,
        )
    assert "worker_test_worker-1" == logger.name
    assert logger.level == logging.DEBUG
    # Cleanup
    for handler in logger.handlers:
        handler.close()
