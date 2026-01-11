module sendit_messenger::generic_store {

    use std::string;
    use std::vector;

    use iota::object::{Self, UID, ID};
    use iota::tx_context::{TxContext, sender};
    use iota::transfer;
    use iota::event;
    use iota::address;

    /// ==========================
    /// SHARED LIGHTWEIGHT REF
    /// ==========================
    public struct ObjectRef has copy, drop, store {
        object_id: ID,
        external_id: string::String,
    }

    /// ==========================
    /// OWNER STRUCT
    /// ==========================
    public struct Owner has key, store {
        id: UID,
        addr: string::String,
        role: string::String,
            removed: bool, // new field to mark deletion
    }

    /// ==========================
    /// OBJECTS
    /// ==========================
    public struct Container has key, store {
        id: UID,
        owners: vector<Owner>,
        external_id: string::String,
        name: string::String,
        description: string::String,
            last_data_item: Option<ID>, // <-- points to the last DataItem
    //    children: vector<ObjectRef>,
    //    data_types: vector<ObjectRef>,
    //    data_items: vector<ObjectRef>,
    }

    public struct DataType has key, store {
        id: UID,
        container: ID,
        external_id: string::String,
        name: string::String,
        description: string::String,
    }

    public struct DataItem has key, store {
        id: UID,
        container: ID,
        data_type: ID,
        creator: string::String,
        external_id: string::String,
        name: string::String,
        content: string::String,
        day_tag: u16, // arbitrary tag now
        prev: Option<ID>, // points to previous DataItem in the container
    }

        public struct ChildContainerLink has key, store {
        id: UID,
        parent_id: ID,
        child_id: ID,
    //    children: vector<ObjectRef>,
    //    data_types: vector<ObjectRef>,
    //    data_items: vector<ObjectRef>,
    }

    /// ==========================
    /// EVENTS
    /// ==========================
    public struct ContainerCreatedEvent has copy, drop {
        object_id: ID,
        external_id: string::String,
        owner: string::String,
        name: string::String,
        description: string::String,
    }

    public struct DataTypeCreatedEvent has copy, drop {
        object_id: ID,
        container_id: ID,
        external_id: string::String,
        name: string::String,
    }

    public struct DataItemCreatedEvent has copy, drop {
        object_id: ID,
        container_id: ID,
        data_type_id: ID,
        external_id: string::String,
        creator: string::String,
        name: string::String,
        day_tag: u16,
    }

    public struct ChildContainerLinkCreatedEvent has copy, drop {
        parent_id: ID,
        child_id: ID,
    }

    /// ==========================
    /// STRING HELPERS
    /// ==========================
    fun string_eq(a: &string::String, b: &string::String): bool {
        let ba = string::bytes(a);
        let bb = string::bytes(b);

        let len = vector::length(ba);
        if (len != vector::length(bb)) {
            return false;
        };

        let mut i = 0;
        while (i < len) {
            if (*vector::borrow(ba, i) != *vector::borrow(bb, i)) {
                return false;
            };
            i = i + 1;
        };

        true
    }

/// ==========================
/// AUTHORIZATION HELPERS
/// ==========================
fun assert_owner(container: &Container, ctx: &TxContext) {
    let caller = make_owner_addr(address::to_string(sender(ctx)));
    let len = vector::length(&container.owners);

    let mut i = 0;
    let mut found = false;

    while (i < len) {
        let owner = vector::borrow(&container.owners, i);
        if (string_eq(&owner.addr, &caller) && !owner.removed) {
            found = true;
            break;
        };
        i = i + 1;
    };

    assert!(found, 1);
}

fun is_owner(container: &Container, addr: &string::String): bool {
    let len = vector::length(&container.owners);
    let mut i = 0;

    while (i < len) {
        let owner = vector::borrow(&container.owners, i);
        if (string_eq(&owner.addr, addr) && !owner.removed) {
            return true;
        };
        i = i + 1;
    };

    false
}

fun make_owner_addr(addr: string::String): string::String {
    let mut s = string::utf8(b"addr:"); // create mutable string
    string::append(&mut s, addr);        // append owned string
    s                                     // return the result
}

/// ==========================
/// CONTAINER OWNER MANAGEMENT
/// ==========================
public entry fun add_owner(
    container: &mut Container,
    new_owner: string::String,  // accept real address
    role: string::String,
    ctx: &mut TxContext
) {
    assert_owner(container, ctx);

    let len = vector::length(&container.owners);
    let mut i = 0;
    let mut found = false;

    while (i < len) {
        let owner = vector::borrow_mut(&mut container.owners, i);
        if (string_eq(&owner.addr, &new_owner)) {
            owner.removed = false; // reactivate removed owner
            owner.role = role;     // optionally update role
            found = true;
            break;
        };
        i = i + 1;
    };

    if (!found) {
        vector::push_back(
            &mut container.owners,
            Owner {
                id: object::new(ctx),
                addr: make_owner_addr(new_owner),
                role,
                removed: false,
            }
        );
    };
}

public entry fun remove_owner(
    container: &mut Container,
    owner_to_remove: string::String, // ← now owned
    ctx: &TxContext
) {
    assert_owner(container, ctx);

    let caller = make_owner_addr(address::to_string(sender(ctx)));
    let len = vector::length(&container.owners);

    // count active owners
    let mut active_count = 0;
    let mut i = 0;
    while (i < len) {
        let owner = vector::borrow(&container.owners, i);
        if (!owner.removed) {
            active_count = active_count + 1;
        };
        i = i + 1;
    };
    assert!(active_count > 1, 100); // cannot remove last active owner

    i = 0;
    let mut found = false;

    while (i < len) {
        let owner = vector::borrow_mut(&mut container.owners, i);
        if (string_eq(&owner.addr, &make_owner_addr(owner_to_remove))) {
            assert!(!string_eq(&caller, &make_owner_addr(owner_to_remove)), 101); // cannot remove yourself
            owner.removed = true;
            found = true;
            break;
        };
        i = i + 1;
    };

    assert!(found, 102); // owner must exist
}

    /// ==========================
    /// CHILD CONTAINERS
    /// ==========================
    public entry fun attach_container_child(
        parent: &mut Container,
        child: &Container,
        ctx: &mut TxContext
    ) {
        assert_owner(parent, ctx);


        let container = ChildContainerLink {
            id: object::new(ctx),
            parent_id: object::id(parent),
            child_id: object::id(child),
        };

/*
        vector::push_back(
            &mut parent.children,
            ObjectRef {
                object_id: object::id(child),
                external_id: child.external_id,
            }
        );*/

        event::emit(ChildContainerLinkCreatedEvent {
            parent_id: object::id(parent),
            child_id: object::id(child)
        });

        transfer::share_object(container);
    }
    /// ==========================
    /// CONTAINER
    /// ==========================
    public entry fun create_container(
        external_id: string::String,
        name: string::String,
        description: string::String,
        ctx: &mut TxContext
    ) {
        let owner_str = make_owner_addr(address::to_string(sender(ctx)));

        let owner = Owner {
            id: object::new(ctx),
            addr: owner_str,
            role: string::utf8(b"owner"),
            removed: false,
        };

        let container = Container {
            id: object::new(ctx),
            owners: vector::singleton(owner),
            external_id,
            name,
            description,
            last_data_item: option::none(),
       /*     children: vector::empty(),
            data_types: vector::empty(),
            data_items: vector::empty(),*/
        };

        let cid = object::id(&container);

        event::emit(ContainerCreatedEvent {
            object_id: cid,
            external_id: container.external_id,
            owner: owner_str,
            name: container.name,
            description: container.description,
        });

        transfer::share_object(container);
    }

    /// ==========================
    /// DATA TYPE
    /// ==========================
    public entry fun create_data_type(
        container: &mut Container,
        external_id: string::String,
        name: string::String,
        description: string::String,
        ctx: &mut TxContext
    ) {
        assert_owner(container, ctx);

        let dt = DataType {
            id: object::new(ctx),
            container: object::id(container),
            external_id,
            name,
            description,
        };

        let dt_id = object::id(&dt);
/*
        vector::push_back(
            &mut container.data_types,
            ObjectRef {
                object_id: dt_id,
                external_id: dt.external_id,
            }
        );
*/
        event::emit(DataTypeCreatedEvent {
            object_id: dt_id,
            container_id: object::id(container),
            external_id: dt.external_id,
            name: dt.name,
        });

        transfer::transfer(dt, sender(ctx));
    }

    /// ==========================
    /// DATA ITEM
    /// ==========================
    public entry fun publish_data_item(
        container: &mut Container,
        data_type: &DataType,
        external_id: string::String,
        name: string::String,
        content: string::String,
        day_tag: u16,
        ctx: &mut TxContext
    ) {
        assert_owner(container, ctx);
        assert!(data_type.container == object::id(container), 2);

        let creator = address::to_string(sender(ctx));

        let item = DataItem {
            id: object::new(ctx),
            container: object::id(container),
            data_type: object::id(data_type),
            creator,
            external_id,
            name,
            content,
            day_tag,
            prev: container.last_data_item,
        };

        let item_id = object::id(&item);
/*
        vector::push_back(
            &mut container.data_items,
            ObjectRef {
                object_id: item_id,
                external_id: item.external_id,
            }
        );
*/
    // update container last pointer
container.last_data_item = option::some(item_id);


        event::emit(DataItemCreatedEvent {
            object_id: item_id,
            container_id: object::id(container),
            data_type_id: object::id(data_type),
            external_id: item.external_id,
            creator: item.creator,
            name: item.name,
            day_tag,
        });

        transfer::transfer(item, sender(ctx));
    }
}
