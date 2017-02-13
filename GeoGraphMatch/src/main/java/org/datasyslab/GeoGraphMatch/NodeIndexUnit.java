package org.datasyslab.GeoGraphMatch;

public class NodeIndexUnit {
	long path_contained_sum;
	long [] path; //the path number corresponding to each candidate
	int size; // the size for both path and candidates
	int [] candidates; //candidate set
	int [][] index = null; //the position based index
	int [] size_of_index = null; // the size of each position based index element. The length of this array equals to its parent node candidate number
	int parent_cand_size;
}
