# Actor onboarding examples. The repository `make check` runs this target.
# The recipe has no shell metacharacters, so GNU make can exec it on a host
# with no /bin/sh. Windows builds use the compiler command in README.md.

.PHONY: check life

check:
	python3 run_examples.py

# POSIX terminal front end; headless tests stay in check.
life:
	python3 run_examples.py --life
