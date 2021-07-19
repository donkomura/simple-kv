#include <exception>
#include <string>

#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include "simplekv.hpp"
#define SIMPLE_KV_LEYOUT "simple-kv"
using runtime_kv_type = simple_kv_runtime<pmem::obj::string, 1024>;
using pmem_kv_type = simple_kv_persistent<pmem::obj::string, 1024>;

struct root {
	pmem::obj::persistent_ptr<pmem_kv_type> kv;
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
	std::string path, op, key, value;

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
				r->kv = pmem::obj::make_persistent<pmem_kv_type>();
			});
		}

		auto runtime_kv = runtime_kv_type(r->kv.get());

		if (op == "get" && argc == 4) {
			std::cout << runtime_kv.get(key).data() << std::endl;
		} else if (op == "put" && argc == 5) {
			value = argv[4];
			runtime_kv.put(key, value);
		} else if (op == "history" && argc == 4) {
			auto v = runtime_kv.history(key);
			for (const auto &e : v) {
				std::cout << e.data() << std::endl;
			}
		} else if (op == "remove" && argc == 4) {
			runtime_kv.remove(key);
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
