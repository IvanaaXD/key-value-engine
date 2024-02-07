package cache

import (
	"container/list"
	"fmt"
	"github.com/IvanaaXD/NASP---Projekat/record"
)

type Cache struct {
	capacity   int
	size       int
	hashMap    map[string]*list.Element
	linkedList *list.List
}

// new cache with size zero

func NewCache(capacity int) *Cache {
	hashMap := make(map[string]*list.Element)
	linkedList := list.New()
	cache := Cache{capacity, 0, hashMap, linkedList}
	return &cache
}

// adding record to cache

func (c *Cache) Add(rec record.Record) {

	element, ok := c.hashMap[rec.Key]

	if ok {
		delete(c.hashMap, element.Value.(record.Record).Key)
		c.linkedList.Remove(element)
		c.linkedList.PushFront(rec)
		c.hashMap[rec.Key] = c.linkedList.Front()
	} else {

		if c.size == c.capacity {
			lastUsed := c.linkedList.Back().Value.(record.Record)
			delete(c.hashMap, lastUsed.Key)
			c.linkedList.Remove(c.linkedList.Back())

			c.linkedList.PushFront(rec)
			c.hashMap[rec.Key] = c.linkedList.Front()

		} else {
			c.linkedList.PushFront(rec)
			c.hashMap[rec.Key] = c.linkedList.Front()
			c.size += 1
		}
	}
}

// deleting record from cache

func (c *Cache) Delete(rec record.Record) {

	element, ok := c.hashMap[rec.Key]

	if ok {
		delete(c.hashMap, rec.Key)
		c.linkedList.Remove(element)
		c.size -= 1
		return
	} else {
		return
	}
}

// looking for record

func (c *Cache) Find(key string) (record.Record, bool) {

	element, ok := c.hashMap[key]

	if ok {
		record := element.Value.(record.Record)
		c.Add(record)
		return record, true
	} else {
		return record.Record{}, false
	}
}

// printing cache

func (c *Cache) Print() {

	for e := c.linkedList.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
	fmt.Println()
}

func (c *Cache) Get(key string) *record.Record {
	element := c.get(key)
	if element != nil {
		return element.Value.(*record.Record)
	}
	return nil
}

func (c *Cache) get(key string) *list.Element {
	value, ok := c.hashMap[key]
	if ok {
		c.linkedList.MoveToFront(value)
		return value
	}
	return nil
}
