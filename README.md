# LoamStream
LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.

**Task Prioritization:**

1. Launching jobs as soon as their dependencies finish
2. Retrying failed jobs
3. Functionality that checks if a prerequisite input was already generated and if so, not run the related job
4. Testing the handling of store aliasing (e.g. specifying base file name for IMPUTE2 which results in several files off of that base name)
5. Enable specification of UGER task options from Loam (DSL) code
6. Clear indication when a pipeline succeeds by making sure all expected output files are generated. If not, fail noisily. 
7. Store auto-generated scripts at a user-specified location instead of UGER nodes' tmp dirs
8. Enable running LoamStream in parallel, possibly by creating .started, .finished files for locking
