# LoamStream
LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.

**Task Prioritization:**

1. Split tasks into multiple jobs when they exceed MAX_NUM_TASKS
2. Record what succeeds and fails, and only run the failed ones for the next time
3. Add the 3rd imputation pipeline step to concat gen (Impute2 output) files using a native Scala tool
4. Launching jobs as soon as their dependencies finish
5. Retrying failed jobs. 
6. Enable specification of UGER task options from Loam (DSL) code
7. Clear indication when a pipeline succeeds by making sure all expected output files are generated. If not, fail noisily. 
8. Enable running LoamStream in parallel, possibly by creating .started, .finished files for locking
