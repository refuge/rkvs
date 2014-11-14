BASE_DIR = $(shell pwd)
SUPPORT_DIR=$(BASE_DIR)/support
ERLC ?= $(shell which erlc)
ESCRIPT ?= $(shell which escript)
ERL ?= $(shell which erl)
APP := enkidb
REBAR?= rebar

$(if $(ERLC),,$(warning "Warning: No Erlang found in your path, this will probably not work"))

$(if $(ESCRIPT),,$(warning "Warning: No escript found in your path, this will probably not work"))

.PHONY: deps doc test

all: deps compile

dev: devbuild

compile:
	@$(REBAR) compile

deps:
	@$(REBAR) get-deps

doc:
	$(REBAR)  -C rebar_dev.config doc skip_deps=true

test:
	$(REBAR) eunit skip_deps=true

clean:
	@$(REBAR) clean
	@rm -f t/*.beam t/temp.*
	@rm -f doc/*.html doc/*.css doc/edoc-info doc/*.png

distclean: clean
	@$(REBAR) delete-deps
	@rm -rf deps

dialyzer: compile
	@dialyzer -Wno_return -c ebin
