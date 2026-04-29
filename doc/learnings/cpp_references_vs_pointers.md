# References vs Pointers in C++

A reference for choosing between `T&` and `T*` in API design. Both give the
caller access to an object without copying it; the difference is about **what
the caller is allowed to assume** and **what the syntax forces them to handle**.

---

## The mechanical difference

```cpp
const std::string& GetTableName() const { return table_name_; }   // reference
const std::string* GetTableName() const { return &table_name_; }  // pointer
```

|                                     | Reference                                          | Pointer                                                  |
| ----------------------------------- | -------------------------------------------------- | -------------------------------------------------------- |
| Can be null                         | No (compiler-enforced)                             | Yes                                                      |
| Can be reassigned to point elsewhere | No                                                 | Yes (if non-const)                                       |
| Caller syntax                       | `r.GetTableName().size()`                          | `r.GetTableName()->size()`                               |
| Implies ownership                   | No                                                 | Ambiguous — could be owned, borrowed, or null             |
| Lifetime contract                   | "Lives at least as long as the source"             | Same, but caller can't assume non-null                   |

---

## The contract is the real difference

When you return `const T&`, you are telling the caller three things:

1. **It exists.** No null check needed.
2. **You don't own it.** Don't `delete` it.
3. **It lives as long as the object you got it from.** If you destroy the
   parent, the reference dangles.

When you return `T*`, you've told the caller almost nothing. They have to ask:
can it be null? Should I free it? How long does it live? That's why
`BufferPoolManager::FetchPage` returns `Page*` — *because* it can return
`nullptr` when the pool is exhausted. The pointer is the way to encode "this
might fail."

---

## Why `LogRecord` returns references

```cpp
const std::string& GetTableName() const { return table_name_; }
const std::vector<int64_t>& GetTuple() const { return tuple_; }
```

Three reasons all at once:

1. **The field always exists** — there's no "missing table name" state. A
   reference is honest about that.
2. **No copy** — returning by value would copy the whole `vector<int64_t>` on
   every call. References hand out a view.
3. **`const` prevents mutation** — the caller can read but not modify,
   preserving encapsulation.

If you had returned `const std::string*`, every call site would need
`if (auto* name = r.GetTableName())` — useless null checks for a field that
can't be null. The reference says "stop worrying, just use it."

---

## When pointers earn their keep

Three legitimate reasons to choose `T*` over `T&`:

### 1. Optionality

```cpp
Page* FetchPage(page_id_t page_id);  // Pool exhaustion → nullptr
```

Pre-C++17, pointer-or-null was the standard "this might not exist" signal.
Post-C++17, `std::optional<T&>` doesn't quite exist (a known wart in the
language), but `T*` still works.

### 2. Rebindable / nullable parameters or members

```cpp
BufferPoolManager(size_t pool_size, DiskManager* disk_manager);
```

A reference member must be initialized in the constructor's init list and can
never be reseated. A pointer member can be null, can be reassigned, and
crucially **doesn't break copy/move** the way a reference member does.
Reference members forbid the implicitly-generated assignment operator —
surprising the first time it bites you.

### 3. Ownership signals

```cpp
void TakeOwnership(std::unique_ptr<Foo> foo);
Foo* foo_raw_ptr;  // observing only, doesn't own
```

Modern C++ uses smart pointers (`unique_ptr`, `shared_ptr`) for owning, raw
pointers for non-owning observation, and references for non-owning observation
that's also non-null. A raw `T*` member should mean "I look at this but don't
free it" — which is exactly what `BufferPoolManager::disk_manager_` is.

---

## The Page/BufferPool boundary illustrates both

In our code:

```cpp
Page* BufferPoolManager::FetchPage(page_id_t page_id);
//   ^ pointer because this can fail (pool exhausted → nullptr)

char* Page::data();
//    ^ pointer because... well, this is a borderline case (see below)

const std::string& LogRecord::GetTableName() const;
//          ^ reference because the field always exists
```

`Page::data()` is interesting — it returns `char*` but it can never be null.
It would be more honest as a reference (`char (&data())[PAGE_SIZE]` if you
want to encode the size in the type — ugly but correct), or as
`std::span<char, PAGE_SIZE>` in C++20. The historical reason for `char*` is
"this is a raw byte buffer for `memcpy`," and `memcpy` takes a pointer.
That's a habit, not a hard requirement.

---

## The lifetime trap

Both references and pointers expose the same trap: the underlying object can
outlive neither.

```cpp
const std::string& Bad() {
    std::string local = "oops";
    return local;  // dangles immediately
}
```

A reference doesn't make this safer than a pointer — it just makes it less
*visible*. The compiler may not warn. This is why returning references to data
members is fine (the member outlives the call), but returning references to
locals is a bug.

The same trap with WAL recovery:

```cpp
auto records = lm.ReadAllLogRecords();
const std::string& name = records[0].GetTableName();
records.clear();  // name now dangles
```

Reference and pointer behave identically here — both dangle. The reference
just looks safer because there's no `nullptr` you'd think to check.

---

## Rule of thumb

- **Returning a member that always exists, no copy wanted:** `const T&`
- **Returning a member that might not exist:** `T*` (or `std::optional<T>` for
  values, or `std::optional<std::reference_wrapper<T>>` if you really need
  optional-reference)
- **Storing a non-owning handle:** raw `T*` (rebindable, nullable, no
  copy/move issues)
- **Storing an owning handle:** `std::unique_ptr<T>` or `std::shared_ptr<T>`
- **Function parameter you'll definitely use, no null possible:** `const T&`
- **Function parameter that might be absent:** `const T*` or
  `std::optional<T>`

---

## In-tree examples

| Decision                                            | Where it shows up                                                |
| --------------------------------------------------- | ---------------------------------------------------------------- |
| `const T&` getter (field always exists, no copy)    | `LogRecord::GetTableName()`, `LogRecord::GetTuple()`             |
| `T*` return (signals "may fail")                    | `BufferPoolManager::FetchPage()` returns `nullptr` on exhaustion |
| Raw `T*` member (non-owning, observing)             | `BufferPoolManager::disk_manager_`                               |
| `std::unique_ptr<T>` (owning)                       | `main.cpp` holding `disk_manager`, `buffer_pool_manager`         |
| Sink parameter (`T` by value + `std::move` in body) | `LogRecord` constructor takes `std::string` / `std::vector` by value |
