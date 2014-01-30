
TEST_RUNNER:=./tests/runTests

export PYTHONPATH=./

compile:
	@echo 'This method is not implemented' 

clean:
	@echo "rm -rf ./dist"; rm -rf ./dist
	@echo "rm -rf ./build"; rm -rf ./build
	@echo "rm -rf *.egg-info"; rm -rf *.egg-info

manual_tests:
	lettuce ./tests/manual/

test:
	@$(TEST_RUNNER)

install: clean
	sudo mkdir -p /opt/blik/fabnet/packages
	python -c "from setup import prepare_install; prepare_install('/opt/blik/fabnet', '/opt/blik/fabnet/fabnet_package_files.lst');"
	python setup.py install --install-lib=/opt/blik/fabnet/packages --prefix=/opt/blik/fabnet --record /opt/blik/fabnet/fabnet_package_files.lst
