sworkflow - Simple Workflow
===========================

sworkflow born inside mydeco to help us solve data processing flows quickly. It's a library to help you create data workflows using Tasks.

What is included?
-----------------

* a set of defined tasks: DumboTask, HDFSOperationTask, PythonTask
* a workflow engine that resolve dependencies and execute the tasks
* a set of utilites to interact with Hadoop File system and create 
  flows of Dumbo tasks.

Requirements
------------

* Python 2.5 or later
* In case you want to use the Dumbo task
    * Dumbo 0.21 or later
    * Hadoop 0.21

TODO
----

* support for parallel tasks
* a task scheduler-dispatcher within workflows
* web interface to see workflow status/scheduled tasks

Authors
-------

mydeco dev team 


