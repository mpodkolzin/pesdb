# Add source subdirectories
add_subdirectory(storage)
add_subdirectory(wal)
add_subdirectory(engine)     # Slice 1: Database (in-memory write API over WAL)
add_subdirectory(recovery)   # Slice 1: replays WAL into Database on startup

# Future subdirectories (uncomment as we implement them)
# add_subdirectory(concurrency) # Phase 5-6: Transactions, MVCC
# add_subdirectory(execution)   # Phase 7: Query operators
# add_subdirectory(main)        # CLI executable (when ready)