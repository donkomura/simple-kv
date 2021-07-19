#include <functional>
#include <iterator>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pexceptions.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/utils.hpp>

#include <boost/type_index.hpp>
#include <stdexcept>

template <typename Value, size_t N>
struct simple_kv_persistent;

/* runtime kv (DRAM)
 * Value : type expected for basic type or pmem::obj container type 
 * N     : hash table bucket count
 */
template <typename Value, std::size_t N>
class simple_kv_runtime {
private:
	using key_type = std::string;
	using bucket_entry_type = std::pair<key_type, std::size_t>;
	using bucket_type = std::vector<bucket_entry_type>;
	using bucket_array_type = std::array<bucket_type, N>;
	using hash_type = std::hash<std::string>;

	bucket_array_type buckets;
	simple_kv_persistent<Value, N> *data;

public:
	/* load persitent kv */
	simple_kv_runtime(simple_kv_persistent<Value, N> *data)
	{
		this->data = data;

		for (std::size_t i = 0; i < data->keys.size(); i++) {
			auto vkey = std::string(data->keys[i].c_str(), data->keys[i].size());
			auto index = hash_type{}(vkey) % N;
			buckets[index].emplace_back(bucket_entry_type{vkey, i});
		}
	}

	const Value &
	get(const std::string &key) const
	{
		auto index = hash_type{}(key) % N;

		/* check all entry for the case of hash collision 
		 * and diff keys in same bucket
		 */
		for (auto itr = buckets[index].rbegin(); itr != buckets[index].rend(); ++itr) {
			if (itr->first == key)
				return data->values[itr->second];
		}

		throw std::out_of_range("no entry in kv");
	}

	std::vector<Value>
	history(const std::string &key) const 
	{
		std::vector<Value> res;
		auto index = hash_type{}(key) % N;

		for (const auto &e : buckets[index]) {
			res.emplace_back(data->values[e.second]);
		}
		return res;
	}

	template <typename T>
	void
	put(const std::string &key, const T &val)
	{
		auto index = hash_type{}(key) % N;
		auto pop = pmem::obj::pool_by_vptr(data);

		/* append only */
		pmem::obj::transaction::run(pop, [&] {
			data->values.emplace_back(val);
			data->keys.emplace_back(key);
		});
		buckets[index].emplace_back(key, data->values.size() - 1);
	}

	void
	remove(const std::string &key) 
	{
		Value nv{};
		put(key, nv);
	}
};

template <typename Value, std::size_t N>
struct simple_kv_persistent
{
	using key_type = pmem::obj::string;
	using key_vector = pmem::obj::vector<key_type>;
	using value_vector = pmem::obj::vector<Value>;

	value_vector values;
	key_vector keys;
};
