#include <functional>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pexceptions.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>
#include <stdexcept>
#include <utility>

template <typename Value, std::size_t N>
class simple_kv {
private:
	using key_type = pmem::obj::string;
	using bucket_type = pmem::obj::vector<std::pair<key_type, Value>>;
	using bucket_array_type = pmem::obj::array<bucket_type, N>;
	using hash_type = std::hash<std::string>;

	bucket_array_type buckets;

public:
	simple_kv() = default;

	const Value &
	get(const std::string &key) const
	{
		auto index = hash_type{}(key) % N;
		return buckets[index].back().second;
	}

	std::vector<Value>
	history(const std::string &key) const 
	{
		std::vector<Value> res;
		auto index = hash_type{}(key) % N;

		for (const auto &e : buckets[index]) {
			res.emplace_back(e.second);
		}
		return res;
	}

	void
	put(const std::string &key, const Value &val)
	{
		auto index = hash_type{}(key) % N;
		auto pop = pmem::obj::pool_by_vptr(this);
		pmem::obj::transaction::run(pop, [&] {
			buckets[index].emplace_back(key, val);
		});
	}

	void
	remove(const std::string &key) 
	{
		Value nv{};
		put(key, nv);
	}
};
