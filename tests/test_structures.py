from BPCon import storage, routing, quorum

def test_storage():
    s = storage.InMemoryStorage()
    s.put("abc", "555")
    assert s.get("abc") == "555"

    
def test_routing():
    r = routing.RoutingManager()
    r.put("127.0.0.1", 8000, 123)
    assert r.get_all() == ["127.0.0.1:8000:123"]

def test_quorum():
    q = quorum.Quorum(5)
    q.add("1")
    q.add("2")
    assert q.is_quorum() == False
    q.add("3")
    assert q.is_quorum() == True
