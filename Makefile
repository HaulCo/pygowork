test:
	mkdir -p .repo/test-logs
	pytest -v --log-file=.repo/test-logs/latest.log --log-file-level=INFO tests

format:
	black src tests

.PHONY: test format
