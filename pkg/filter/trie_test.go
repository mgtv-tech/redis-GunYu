package filter

import "testing"

func TestTrieIsPrefixMatch(t *testing.T) {
	trie := NewTrie()

	prefixes := []string{"apple", "app", "ball", "c", "ccc"}
	for _, p := range prefixes {
		trie.Insert(p)
	}

	checkWords := map[string]bool{
		"a":     false,
		"ap":    false,
		"app":   true,
		"appl":  true,
		"appla": true,
		"apx":   false,
		"ban":   false,
		"bal":   false,
		"ball":  true,
		"c":     true,
		"cc":    true,
	}
	for word, expected := range checkWords {
		if trie.IsPrefixMatch(word) != expected {
			t.Fatalf("%s : expected(%v), actual(%v)", word, expected, trie.IsPrefixMatch(word))
		}
	}
}

func TestTrieSearch(t *testing.T) {
	trie := NewTrie()

	keyWords := []string{"apple", "app", "ball", "c", "ccc"}
	for _, p := range keyWords {
		trie.Insert(p)
	}

	checkWords := map[string]bool{
		"a":     false,
		"ap":    false,
		"app":   true,
		"appl":  false,
		"appla": false,
		"apx":   false,
		"ban":   false,
		"bal":   false,
		"ball":  true,
		"c":     true,
		"cc":    false,
	}
	for word, expected := range checkWords {
		if trie.Search(word) != expected {
			t.Fatalf("%s : expected(%v), actual(%v)", word, expected, trie.Search(word))
		}
	}
}
