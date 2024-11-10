package main

import (
	lru "github.com/hashicorp/golang-lru/v2"
)

var cacheFileMap, _ = lru.New[string, any](CACHE_SIZE)

func SetCache(hdfsFileName string) {
	if !CACHE_ENABLED {
		return
	}

	if cacheFileMap.Len() == CACHE_SIZE {
		oldestFile, _, _ := cacheFileMap.RemoveOldest()
		delete(tempFileInfoMap, oldestFile)
		delete(tempFileBlockMap, oldestFile)
	}

	cacheFileMap.Add(hdfsFileName, 0)
}

func GetCache(hdfsFileName string) bool {
	if !CACHE_ENABLED {
		return false
	}

	_, ok := cacheFileMap.Get(hdfsFileName)

	return ok
}
