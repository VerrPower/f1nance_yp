
#pragma once

#include <ostream>
#include <cassert>

template<typename K, typename V>
class HashTable{

    typedef struct Entry{
        int hash_value;
        K key;
        V value;
    }Entry;

private:
    static constexpr int MIN_CAPACITY = 8;
    static constexpr int DELETED_BIT = -2;
    static constexpr int UNVISITED_BIT = -1;
    static constexpr double MAX_LOAD_FACTOR = 2.0 / 3.0;
    static constexpr double MIN_LOAD_FACTOR = 1.0 / 6.0;
    static constexpr int PERTURB_SHIFT = 5;
    
    int hash_func(const K& key);
    void ensure_capacity();
    void resize_to(int new_cap);
    
    int *indices;
    Entry *entries;
    int capacity, num_entry, num_active_entry;
    int expand_threshold, shrink_threshold;
    
public:
    HashTable();
    ~HashTable();
    void insert(const K& key, const V& value);
    bool contains(const K& key);
    V pop(const K& key);
    V* get(const K& key);

    int size();
    
    template<typename K, typename V>
    friend std::ostream& operator<<(std::ostream& os, HashTable<K, V>& tab);
};

#include "HashTable.tpp"
