# Add source subdirectories
add_subdirectory(storage)

# Future subdirectories (uncomment as we implement them)
# add_subdirectory(recovery)   # Phase 4: WAL, recovery
# add_subdirectory(concurrency) # Phase 5-6: Transactions, MVCC
# add_subdirectory(execution)   # Phase 7: Query operators
# add_subdirectory(engine)      # Phase 3: Query executor
# add_subdirectory(main)        # CLI executable (when ready)