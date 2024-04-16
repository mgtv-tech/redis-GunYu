package filter

type TrieNode struct {
	children map[rune]*TrieNode
	isEnd    bool
}

// Trie, not concurrency safe
type Trie struct {
	root *TrieNode
}

func NewTrie() *Trie {
	return &Trie{
		root: &TrieNode{
			children: make(map[rune]*TrieNode),
			isEnd:    false,
		},
	}
}

func (t *Trie) Insert(word string) {
	node := t.root
	for _, ch := range word {
		if node.children[ch] == nil {
			node.children[ch] = &TrieNode{
				children: make(map[rune]*TrieNode),
				isEnd:    false,
			}
		}
		node = node.children[ch]
	}
	node.isEnd = true
}

func (t *Trie) IsPrefixMatch(word string) bool {
	node := t.root
	for _, ch := range word {
		node = node.children[ch]
		if node == nil {
			return false
		}
		if node.isEnd {
			return true
		}
	}
	return false
}

func (t *Trie) Search(word string) bool {
	node := t.root
	for _, ch := range word {
		node = node.children[ch]
		if node == nil {
			return false
		}
	}
	return node.isEnd
}
