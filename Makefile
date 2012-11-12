
TEST_RUNNER:=./tests/runTests

export PYTHONPATH=./

compile:
	@echo 'This method is not implemented' 

clean:
	@echo "rm -rf ./dist"; rm -rf ./dist

manual_tests:
	lettuce ./tests/manual/

test:
	@$(TEST_RUNNER)
