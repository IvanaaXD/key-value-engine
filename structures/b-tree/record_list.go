package b_tree

import "github.com/IvanaaXD/NASP---Projekat/record"

type RecordList struct {
	recordList []*record.Record
}

// getting data from nodes

func (rl *RecordList) getRecords(node *BTreeNode) {

	for _, p := range node.record {
		rl.recordList = append(rl.recordList, p)

		if len(node.child) > 0 {
			for _, v := range node.child {
				rl.getRecords(v)
			}
		}
	}
}
