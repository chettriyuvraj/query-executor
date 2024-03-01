package btree

type BPlusTree struct {
	root   *BPlusTreeNode
	degree int
}

type BPlusTreeNode interface {
	find(val string) *BPlusTreeLeafNode
}

type BPlusTreeIndexNode struct {
	elems []*BPlusTreeIndexElem
}

type BPlusTreeIndexElem struct {
	ptr *BPlusTreeNode // left ptr to the elem i.e before the elem
	val string
}

type BPlusTreeLeafNode struct {
	rightPtr *BPlusTreeLeafNode // ptr to the adj leaf node
	elems    []*BPlusTreeLeafElem
}

type BPlusTreeLeafElem struct {
	val          string
	recordNumber int
}
