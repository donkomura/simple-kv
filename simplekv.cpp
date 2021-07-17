#include <exception>
#include <iostream>
#include <libpmemobj++/persistent_ptr_base.hpp>
#include <stdexcept>
#include <string>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include "simplekv.hpp"
#define SIMPLE_KV_LEYOUT "simple-kv"
using kv_type = simple_kv<int, 1024>;

struct root {
	pmem::obj::persistent_ptr<kv_type> kv;
};

void
usage(std::string diag)
{
	std::cerr << "usage: "
		<< diag << " [path-to-poolfile]"
		<< " [get key | put key value | history key]" << std::endl;
}

int
main(int argc, char **argv)
{
	std::string path, op, key;
	int value;

	if (argc < 3) {
		usage("simple-kv");
		exit(1);
	}

	path  = argv[1];
	op    = argv[2];
	key   = argv[3];
	pmem::obj::pool<root> pop;

	try {
		pop = pmem::obj::pool<root>::open(path, SIMPLE_KV_LEYOUT);
		auto r = pop.root();

		if (r->kv == nullptr) {
			pmem::obj::transaction::run(pop, [&]{
				r->kv = pmem::obj::make_persistent<kv_type>();
			});
		}

		if (op == "get" && argc == 4)
			std::cout << r->kv->get(key) << std::endl;
		else if (op == "put" && argc == 5) {
			value = atoi(argv[4]);
			r->kv->put(key, value);
		} else if (op == "history" && argc == 4) {
			auto v = r->kv->history(key);
			for (const auto &e : v) {
				std::cout << e << std::endl;
			}
		} else if (op == "remove" && argc == 4) {
			r->kv->remove(key);
		} else {
			usage("simple-kv");
			pop.close();
			return 1;
		}
	} catch (pmem::pool_error &e) {
		std::cerr << e.what() << std::endl;
		std::cerr
			<< "To create pool : pmempool create obj --layout=" << SIMPLE_KV_LEYOUT
			<< " -s <some size> " << path
			<< std::endl;
	} catch (std::exception &e) {
		std::cerr << e.what() << std::endl;
	}

	try {
		pop.close();
	} catch (const std::logic_error &e) {
		std::cerr << e.what() << std::endl;
	}
	return 0;
}
