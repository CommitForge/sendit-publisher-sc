module sendit_messenger::generic_store {
    use std::string;
    use std::vector;
    use iota::object::{Self, UID, ID};
    use iota::tx_context::{TxContext, sender};
    use iota::transfer;
    use iota::event;
    use iota::address;

    /// ==========================
    /// OWNER STRUCT/// ==========================
/// OWNERS
/// ==========================
public struct Owner has key, store {
    id: UID,
    addr: string::String,
    role: string::String,
    removed: bool,
}

public struct OwnerTrash has key, store {
    id: UID,
    container_id: ID,
    owners: vector<ID>, // only store IDs of owners
}

/// ==========================
/// OBJECTS
/// ==========================
public struct Container has key, store {
    id: UID,
    external_id: string::String,
    owners: vector<Owner>,
    name: string::String,
    description: string::String,
    content: string::String,
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
    last_owner: Option<ID>,
    last_child: Option<ID>,
    last_data_type: Option<ID>,
    last_data_item: Option<ID>,
}

public struct DataType has key, store {
    id: UID,
    container_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    schemas: string::String,
    tag_index: u256,
    prev: Option<ID>,
    prevDataItem: Option<ID>,
}

public struct DataItem has key, store {
    id: UID,
    container_id: ID,
    data_type_id: ID,
    creator: string::String,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    tag_index: u256,
    prev: Option<ID>,
}

public struct ChildContainerLink has key, store {
    id: UID,
    parent_container_id: ID,
    child_container_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
        prev: Option<ID>,
}

/// ==========================
/// EVENTS (FULL OBJECT SNAPSHOT)
/// ==========================
public struct ContainerCreatedEvent has copy, drop {
    object_id: ID,
    external_id: string::String,
    owners: vector<string::String>,  // addresses
    name: string::String,
    description: string::String,
    content: string::String,
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
    last_owner: Option<ID>,
    last_child: Option<ID>,
    last_data_type: Option<ID>,
    last_data_item: Option<ID>,
}

public struct DataTypeCreatedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    schemas: string::String,
    tag_index: u256,
    prev: Option<ID>,
    prevDataItem: Option<ID>,
}

public struct DataItemCreatedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    data_type_id: ID,
    creator: string::String,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    tag_index: u256,
    prev: Option<ID>,
}

public struct ChildContainerLinkCreatedEvent has copy, drop {
    id: ID,
    parent_id: ID,
    child_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
        prev: Option<ID>,
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
const E_NOT_OWNER: u64 = 0x100;

fun assert_owner(container: &Container, asserted: bool, ctx: &TxContext) {
if (!asserted) {
    let caller = make_owner_addr(address::to_string(sender(ctx)));
    let len = vector::length(&container.owners);
    let mut i = 0;

    while (i < len) {
        let owner_ref = vector::borrow(&container.owners, i);
        if (!owner_ref.removed && string_eq(&owner_ref.addr, &caller)) {
            return; // authorized, exit early
        };
        i = i + 1;
    };

    // If we get here, no owner matched
    abort E_NOT_OWNER;
}
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
    assert_owner(container, container.public_update_container, ctx);

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
    assert_owner(container, container.public_update_container, ctx);

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
    parent_container: &mut Container,
    child_container: &Container,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    ctx: &mut TxContext,
) {
    assert_owner(parent_container, parent_container.public_attach_container_child, ctx);

    let container = ChildContainerLink {
        id: object::new(ctx),
        parent_container_id: object::id(parent_container),
        child_container_id: object::id(child_container),
        external_id: external_id,
        name: name,
        description: description,
        content: content,
        prev: parent_container.last_child,
    };

    let container_id = object::id(&container);
   parent_container.last_child = option::some(container_id);


    // Emit full snapshot event
    event::emit(ChildContainerLinkCreatedEvent {
        id: object::id(&container),
        parent_id: object::id(parent_container),
        child_id: object::id(child_container),
        external_id: container.external_id,
        name: container.name,
        description: container.description,
        content: container.content,
         prev: container.prev,
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
    content: string::String,
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
    ctx: &mut TxContext
) {
    let owner_addr = make_owner_addr(address::to_string(sender(ctx)));

    // Create owner object
    let owner = Owner {
        id: object::new(ctx),
        addr: owner_addr,
        role: string::utf8(b"owner"),
        removed: false,
    };

    let owner_id = object::id(&owner);

    // Create container
    let container = Container {
        id: object::new(ctx),
        owners: vector::singleton(owner),
        external_id,
        name,
        description,
        content,
        public_update_container,
        public_attach_container_child,
        public_create_data_type,
        public_publish_data_item,
        last_owner: option::some(owner_id),
        last_child: option::none(),
        last_data_type: option::none(),
        last_data_item: option::none(),
    };

    let cid = object::id(&container);

    // --- Emit full snapshot event ---
    let mut owner_addrs = vector::empty<string::String>();
    let len = vector::length(&container.owners);
    let mut i = 0;
    while (i < len) {
        let owner_ref = vector::borrow(&container.owners, i);
        vector::push_back(&mut owner_addrs, owner_ref.addr);
        i = i + 1;
    };

    event::emit(ContainerCreatedEvent {
        object_id: cid,
        external_id: container.external_id,
        owners: owner_addrs,
        name: container.name,
        description: container.description,
        content: container.content,
        public_update_container: container.public_update_container,
        public_attach_container_child: container.public_attach_container_child,
        public_create_data_type: container.public_create_data_type,
        public_publish_data_item: container.public_publish_data_item,
        last_owner: container.last_owner,
        last_child: container.last_child,
        last_data_type: container.last_data_type,
        last_data_item: container.last_data_item,
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
    content: string::String,
        schemas: string::String,
        tag_index: u256,
    ctx: &mut TxContext
) {
    assert_owner(container, container.public_create_data_type, ctx);

    let dt = DataType {
        id: object::new(ctx),
        container_id: object::id(container),
        external_id,
        name,
        description,
        content: content,
        schemas: schemas,
        tag_index: tag_index,
        prev: container.last_data_type,
        prevDataItem: container.last_data_item,
    };

    let dt_id = object::id(&dt);
    container.last_data_type = option::some(dt_id);

    // Emit full snapshot
    event::emit(DataTypeCreatedEvent {
        object_id: dt_id,
        container_id: object::id(container),
        external_id: dt.external_id,
        name: dt.name,
        description: dt.description,
        content: dt.content,
        schemas: schemas,
        tag_index: dt.tag_index,
        prev: dt.prev,
        prevDataItem: dt.prevDataItem,
    });

    transfer::share_object(dt);
}

/// ==========================
/// DATA ITEM
/// ==========================
const E_INVALID_DATATYPE: u64 = 1001;
public entry fun publish_data_item(
    container: &mut Container,
    data_type: &mut DataType,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    tag_index: u256,
    ctx: &mut TxContext
) {
    assert_owner(container, container.public_publish_data_item, ctx);

    assert!(data_type.container_id == object::id(container), E_INVALID_DATATYPE);

    let creator = address::to_string(sender(ctx));

    let item = DataItem {
        id: object::new(ctx),
        container_id: object::id(container),
        data_type_id: object::id(data_type),
        creator,
        external_id,
        name,
        description,
        content,
        tag_index,
        prev: container.last_data_item,
    };

    let item_id = object::id(&item);

    container.last_data_item = option::some(item_id);
    data_type.prevDataItem = option::some(item_id);

    // Emit full snapshot
    event::emit(DataItemCreatedEvent {
        object_id: item_id,
        container_id: object::id(container),
        data_type_id: object::id(data_type),
        creator: item.creator,
        external_id: item.external_id,
        name: item.name,
        description: item.description,
        content: item.content,
        tag_index: item.tag_index,
        prev: item.prev,
    });

    transfer::share_object(item);
}

/// ==========================
/// UPDATE METHODS
/// ==========================


    // Update container
    public entry fun update_container(
        container: &mut Container,
        new_name: string::String,
        new_description: string::String,
        new_content: string::String,
        new_external_id: string::String,
    ctx: &mut TxContext
    ) {
         assert_owner(container, container.public_update_container, ctx);
   
            container.name = new_name;
        
     
            container.description = new_description;
        
       
            container.content = new_content;
       
      
            container.external_id = new_external_id;
         
    }

    // Update data type
    public entry fun update_data_type(
        container: &mut Container,
        data_type: &mut DataType,
        new_external_id: string::String,
        new_name: string::String,
        new_description: string::String,
        new_content: string::String,
        new_schemas: string::String,
        new_tag_index: u256,
    ctx: &mut TxContext
    ) {
        assert_owner(container, container.public_create_data_type, ctx);
        assert!(data_type.container_id == object::id(container), E_INVALID_DATATYPE);

data_type.external_id = new_external_id;

            data_type.name = new_name;
        
            data_type.description = new_description;

            data_type.content = new_content;

             data_type.schemas = new_schemas;
      
            data_type.tag_index = new_tag_index;
     
            
    
    }










}
