package serverless

import (
	"fmt"
)

type TrieNode interface {
	FindPartition(key string) int
}

type LeafTrieNode struct {
	lower       int
	upper       int
	splitPoints []string
	level       int
}

func NewLeafTrieNode(level int, splitPoints []string, lower int, upper int) (trieNode *LeafTrieNode) {
	trieNode = new(LeafTrieNode)
	trieNode.level = level
	trieNode.splitPoints = splitPoints
	trieNode.lower = lower
	trieNode.upper = upper

	return
}

func (trieNode *LeafTrieNode) FindPartition(key string) int {
	for i := trieNode.lower; i < trieNode.upper; i++ {
		if trieNode.splitPoints[i] >= key {
			return i
		}
	}

	return trieNode.upper
}

type InnerTrieNode struct {
	child []TrieNode
	level int
}

func NewInnerTrieNode(level int) (trieNode *InnerTrieNode) {
	trieNode = new(InnerTrieNode)
	trieNode.level = level
	trieNode.child = make([]TrieNode, 256)
	return
}

func (trieNode *InnerTrieNode) FindPartition(key string) int {
	var level int
	level = trieNode.level

	if len(key) <= level {
		return trieNode.child[0].FindPartition(key)
	}

	return trieNode.child[[]byte(key)[level]].FindPartition(key)
}

func (trieNode *InnerTrieNode) SetChild(idx int, child TrieNode) {
	trieNode.child[idx] = child
}

func GetPartition(key string, trie TrieNode) int {
	return trie.FindPartition(key)
}

func BuildTrie(splits []string, lower int, upper int, prefix string, maxDepth int) TrieNode {
	var depth int
	var trieNode *InnerTrieNode
	var leafTrieNode *LeafTrieNode
	var trial string
	var currentBound int

	trieNode = NewInnerTrieNode(depth)

	depth = len([]byte(prefix))
	trial = prefix + string([]byte{1})
	currentBound = lower

	fmt.Println("=== Initial Values =================")
	fmt.Printf("trial (bytes) = %v\n", []byte(trial))
	fmt.Printf("depth = %d\n", depth)
	fmt.Printf("lower = %d\nupper = %d\nprefix = %v\nmaxDepth = %d\ncurrentBound = %d\n", lower, upper, prefix, maxDepth, currentBound)
	fmt.Println("------------------------------------")

	if depth >= maxDepth || lower == upper {
		fmt.Println("Returning leaf node.")
		leafTrieNode = NewLeafTrieNode(depth, splits, lower, upper)
		return leafTrieNode
	}

	// The loop is supposed to be ++ch
	for ch := 0; ch < 255; ch++ {
		asBytes := []byte(trial)

		asBytes[depth] = byte(ch + 1)
		trial = string(asBytes) // trial.getBytes()[depth] = (byte) (ch + 1);

		lower = currentBound
		for currentBound < upper {
			if splits[currentBound] >= trial {
				break
			}

			currentBound = currentBound + 1
		}
		asBytes = []byte(trial)
		fmt.Printf("Length of Trial (str): %d, Length of Trial (bytes): %d\n", len(trial), len(asBytes))
		fmt.Printf("depth = %d\n", depth)
		fmt.Printf("currentBound = %d\n", currentBound)
		fmt.Printf("ch = %d\n", ch)
		asBytes[depth] = byte(ch)
		trial = string(asBytes)
		trieNode.child[ch] = BuildTrie(splits, lower, currentBound, trial, maxDepth)
	}

	asBytes := []byte(trial)
	asBytes[depth] = byte(127)
	trial = string(asBytes)
	trieNode.child[255] = BuildTrie(splits, currentBound, upper, trial, maxDepth)

	fmt.Println("Returning the constructed Trie now...")
	return trieNode
}
