# master-worker-pattern
Master-Worker pattern with cache using akka actors

# Description
Master-Worker pattern with a cache where completed jobs are stored. Newly submitted jobs are stored in a hash map and a queue.
If at a later time a job will be submitted that has already been put into the hash map, its sender will be added to the list of actors waiting for this job to complete. The amount of workers can be adjusted during runtime by sending appropriate messages. Finally there is a possibility to control what happens after a worker dies.

# Usage
See master-worker-pattern/pattern/src/pattern/Example.scala

# Contact
E-mail: kuba.weimann@gmail.com
