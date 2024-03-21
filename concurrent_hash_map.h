#pragma once

#include <unordered_map>
#include <mutex>
#include <vector>
#include <atomic>
#include <algorithm>
#include <stdexcept>

template <class K, class V, class Hash = std::hash<K>>
class ConcurrentHashMap {
private:
    void Rehash() {
        for (auto& m : mutex_) {
            m.lock();
        }
        if ((size_ + buckets_ - 1) / buckets_ > kLoadFactor) {
            size_t new_bucket_cnt;
            if (size_ % mutex_.size() == 0) {
                new_bucket_cnt = size_;
            } else {
                new_bucket_cnt = size_ + mutex_.size() - (size_ % mutex_.size());
            }
            std::vector<std::vector<std::pair<K, V>>> temp(new_bucket_cnt);
            for (auto& vec : table_) {
                for (auto& el : vec) {
                    uint64_t hash = hasher_(el.first);
                    size_t idx = hash % new_bucket_cnt;
                    temp[idx].push_back(std::move(el));
                }
            }
            table_ = std::move(temp);
            buckets_ = new_bucket_cnt;
        }
        for (auto& m : mutex_) {
            m.unlock();
        }
    }

public:
    explicit ConcurrentHashMap(const Hash& hasher = {})
        : ConcurrentHashMap(kUndefinedSize, kDefaultConcurrencyLevel, hasher) {
    }

    explicit ConcurrentHashMap(int expected_size, const Hash& hasher = {})
        : ConcurrentHashMap(expected_size, kDefaultConcurrencyLevel, hasher) {
    }

    ConcurrentHashMap(int expected_size, int expected_threads_count, const Hash& hasher = {})
        : hasher_(hasher) {
        size_t threads_cnt = std::max(8, expected_threads_count);
        mutex_ = std::vector<std::mutex>(threads_cnt);
        if (expected_size == kUndefinedSize) {
            table_.resize(threads_cnt);
        } else {
            size_t size;
            expected_size = (expected_size + kLoadFactor - 1) / kLoadFactor;
            if (expected_size % threads_cnt == 0) {
                size = expected_size;
            } else {
                size = expected_size + threads_cnt - (expected_size % threads_cnt);
            }
            table_.resize(size);
            for (auto& vec : table_) {
                vec.reserve(kLoadFactor);
            }
        }
        size_ = 0;
        buckets_ = table_.size();
    }

    bool Insert(const K& key, const V& value) {
        uint64_t hash = hasher_(key);
        size_t m_idx = hash % mutex_.size();
        {
            std::lock_guard lg(mutex_[m_idx]);
            size_t idx = hash % buckets_;
            for (const auto& el : table_[idx]) {
                if (el.first == key) {
                    return false;
                }
            }
            table_[idx].push_back({key, value});
            size_++;
        }
        size_t size_copy = size_;
        size_t buckets_copy = buckets_;
        if ((size_copy + buckets_copy - 1) / buckets_copy > kLoadFactor) {
            Rehash();
        }
        return true;
    }

    bool Erase(const K& key) {
        uint64_t hash = hasher_(key);
        size_t m_idx = hash % mutex_.size();
        std::lock_guard lg(mutex_[m_idx]);
        size_t idx = hash % buckets_;
        size_t el_idx = 0;
        for (const auto& el : table_[idx]) {
            if (el.first == key) {
                break;
            }
            el_idx++;
        }
        if (el_idx == table_[idx].size()) {
            return false;
        }
        table_[idx].erase(table_[idx].begin() + el_idx);
        size_--;
        return true;
    }

    void Clear() {
        for (auto& m : mutex_) {
            m.lock();
        }
        for (auto& vec : table_) {
            vec.clear();
        }
        size_ = 0;
        for (auto& m : mutex_) {
            m.unlock();
        }
    }

    std::pair<bool, V> Find(const K& key) const {
        uint64_t hash = hasher_(key);
        size_t m_idx = hash % mutex_.size();
        std::lock_guard lg(mutex_[m_idx]);
        size_t idx = hash % buckets_;
        for (const auto& el : table_[idx]) {
            if (el.first == key) {
                return {true, el.second};
            }
        }
        return {false, {}};
    }

    V At(const K& key) const {
        uint64_t hash = hasher_(key);
        size_t m_idx = hash % mutex_.size();
        std::lock_guard lg(mutex_[m_idx]);
        size_t idx = hash % buckets_;
        for (const auto& el : table_[idx]) {
            if (el.first == key) {
                return el.second;
            }
        }
        throw std::out_of_range("No such element");
    }

    size_t Size() const {
        return size_;
    }

    static constexpr auto kUndefinedSize = -1;

private:
    static constexpr auto kDefaultConcurrencyLevel = 8;
    static constexpr auto kLoadFactor = 3;

    std::vector<std::vector<std::pair<K, V>>> table_;
    mutable std::vector<std::mutex> mutex_;
    std::atomic<size_t> size_;
    std::atomic<size_t> buckets_;
    Hash hasher_;
};
