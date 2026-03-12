# Migration Guide: Clean Slate Setup

## Step 1: Archive Old Code

Run these commands from the project root:

```bash
# Create archive directory
mkdir -p old/include old/src

# Archive old headers
mv include/columnar_db/engine old/include/
mv include/columnar_db/storage/buffer_pool_manager.h old/include/
mv include/columnar_db/storage/catalog.h old/include/
mv include/columnar_db/storage/disk_manager.h old/include/
mv include/columnar_db/storage/page.h old/include/
mv include/columnar_db/storage/table.h old/include/

# Archive old implementations
mv src/engine old/src/
mv src/storage old/src/
mv src/wal old/src/
mv src/main/main.cpp old/src/

# Archive old CMake files (we'll write fresh ones)
mv src/CMakeLists.txt old/
mv src/common/CMakeLists.txt old/ 2>/dev/null || true
mv src/main/CMakeLists.txt old/ 2>/dev/null || true

# Clean up empty directories
rmdir include/columnar_db/engine 2>/dev/null || true
rmdir include/columnar_db/storage 2>/dev/null || true
```

## Step 2: What We're Keeping

We'll keep (and improve) these foundation files:
- `include/columnar_db/common/config.h` - Basic constants (will enhance)
- `include/columnar_db/common/types.h` - Type definitions (will enhance)

## Step 3: Verify Clean State

After archiving, you should have:

```
pesdb/
├── old/                    # ✅ Archived code (reference only)
├── include/columnar_db/
│   └── common/            # ✅ Only common/ exists
│       ├── config.h
│       └── types.h
├── src/
│   └── main/              # ✅ Empty, ready for new main.cpp
└── doc/                   # ✅ Our documentation
```

## Step 4: Ready to Build

Once archiving is complete, we'll create:
1. Clean directory structure
2. Phase 1 components (Disk Manager, Page, Buffer Pool)
3. Unit tests with Google Test
4. Fresh CMake configuration

---

**Status**: Ready to execute Step 1 (archive old code)
**Next**: Run the bash commands above, then tell me when done!
