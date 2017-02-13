package org.datasyslab.GeoGraphMatch;

public class NEC_element {
	Integer label;
	int parent_id;
	int represent_node;

	NEC_element() {

	}

	NEC_element(int label, int parent_id, int represent_node) {
		this.label = label;
		this.parent_id = parent_id;
		this.represent_node = represent_node;
	}
}
