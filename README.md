# Graph-Processing-on-Spark
The purpose of this project is to develop a graph analysis program using Apache Spark.

The graph can be represented as RDD[ ( Long, Long, List[Long] ) ], where the first Long is the graph node ID, the second Long is the assigned cluster ID (-1 if the node has not been assigned yet), and the List[Long] is the adjacent list (the IDs of the neighbors). Here is the pseudo-code:

var graph = /* read graph from args(0); the graph cluster ID is set to -1 except for the first 5 nodes */

for (i <- 1 to depth)
   graph = graph.flatMap{ /* (1) */ }
                .reduceByKey(_ max _)
                .join( graph.map( /* (2) */ ) )
                .map{ /* (3) */ }

/* finally, print the partition sizes */
where the mapper function (1) takes a node ( id, cluster, adjacent) in the graph and returns (id,cluster) along with all (x,cluster) for all x in adjacent. Then the join joins the result with the graph (after it is mapped with function (2)). The join returns an RDD of tuples (id,(new,(old,adjacent))) with the new and the old cluster numbers. In function (3) you keep the old cluster of it's not -1, otherwise you use the new.
