module sendit_messenger::generic_store;

use iota::address;
use iota::event;
use iota::object::{Self, UID, ID};
use iota::transfer;
use iota::tx_context::{TxContext, sender};
use std::string;
use std::vector;

// true max 340282366920938463463374607431768211455
const MAX_u128: u128 = 340282366920938463463374607431768211450;

/// ==========================
/// OWNERS
/// ==========================
public struct Owner has key, store {
    id: UID,
    addr: string::String,
    role: string::String,
    removed: bool,
    sequence_index: u128,
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
    sequence_index: u128,
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
    last_owner_id: Option<ID>,
    last_child_id: Option<ID>,
    last_data_type_id: Option<ID>,
    last_data_item_id: Option<ID>,
    last_owner_index: u128,
    last_child_index: u128,
    last_data_type_index: u128,
    last_data_item_index: u128,
}

public struct DataType has key, store {
    id: UID,
    container_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    schemas: string::String,
    sequence_index: u128,
    tag_index: u128,
    last_data_item_id: Option<ID>,
    prev_id: Option<ID>,
}

public struct DataItem has key, store {
    id: UID,
    container_id: ID,
    data_type_id: ID,
    external_id: string::String,
    creator: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    tag_index: u128,
    prev_id: Option<ID>,
    prev_data_type_item_id: Option<ID>,
}

public struct ChildContainerLink has key, store {
    id: UID,
    parent_container_id: ID,
    child_container_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    prev_id: Option<ID>,
}

/// ==========================
/// EVENTS (FULL OBJECT SNAPSHOT)
/// ==========================
public struct ContainerCreatedEvent has copy, drop {
    object_id: ID,
    external_id: string::String,
    owners: vector<string::String>, // addresses
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
    last_owner_id: Option<ID>,
    last_child_id: Option<ID>,
    last_data_type_id: Option<ID>,
    last_data_item_id: Option<ID>,
    last_owner_index: u128,
    last_child_index: u128,
    last_data_type_index: u128,
    last_data_item_index: u128,
}

public struct DataTypeCreatedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    schemas: string::String,
    sequence_index: u128,
    tag_index: u128,
    last_data_item_id: Option<ID>,
    prev_id: Option<ID>,
}

public struct DataItemPublishedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    data_type_id: ID,
    external_id: string::String,
    creator: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    tag_index: u128,
    prev_id: Option<ID>,
    prev_data_type_item_id: Option<ID>,
}

public struct ChildContainerLinkAttachedEvent has copy, drop {
    id: ID,
    parent_container_id: ID,
    child_container_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    prev_id: Option<ID>,
}

public struct OwnerAddedEvent has copy, drop {
    container_id: ID,
    owner_addr: string::String,
    role: string::String,
    sequence_index: u128,
}

public struct OwnerRemovedEvent has copy, drop {
    container_id: ID,
    owner_addr: string::String,
    sequence_index: u128,
}

public struct ContainerUpdatedEvent has copy, drop {
    object_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
}
public struct DataTypeUpdatedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    schemas: string::String,
    sequence_index: u128,
    tag_index: u128,
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
const E_NOT_OWNER: u64 = 0x1000;

fun assert_owner(container: &Container, asserted: bool, ctx: &TxContext) {
    if (!asserted) {
        let caller = make_owner_addr(address::to_string(sender(ctx)));
        let len = vector::length(&container.owners);
        let mut i = 0;

        while (i < len) {
            let owner = vector::borrow(&container.owners, i);
            if (!owner.removed && string_eq(&owner.addr, &caller)) {
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
    string::append(&mut s, addr); // append owned string
    s // return the result
}

public fun add_with_wrap(val: u128, add: u128): u128 {
    let sum = val + add;
    if (sum > MAX_u128) {
        1
    } else {
        sum
    }
}

/// ==========================
/// CONTAINER OWNER MANAGEMENT
/// ==========================
public entry fun add_owner(
    container: &mut Container,
    new_owner: string::String,
    role: string::String,
    ctx: &mut TxContext,
) {
    assert_owner(container, container.public_update_container, ctx);

    let next_index = add_with_wrap(container.last_owner_index, 1);
    container.last_owner_index = next_index;

    let owner_addr = make_owner_addr(new_owner);

    let len = vector::length(&container.owners);
    let mut i = 0;
    let mut found = false;

    while (i < len) {
        let owner = vector::borrow_mut(&mut container.owners, i);
        if (string_eq(&owner.addr, &owner_addr)) {
            owner.removed = false;
            owner.role = role;
            owner.sequence_index = next_index;
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
                addr: owner_addr,
                role,
                removed: false,
                sequence_index: next_index,
            },
        );
    };

    event::emit(OwnerAddedEvent {
        container_id: object::id(container),
        owner_addr,
        role,
        sequence_index: next_index,
    });
}

const E_CANNOT_REMOVE_LAST_OWNER: u64 = 1002;
const E_CANNOT_REMOVE_SELF: u64 = 1003;
const E_OWNER_NOT_FOUND: u64 = 1004;

public entry fun remove_owner(
    container: &mut Container,
    owner_to_remove: string::String,
    ctx: &TxContext,
) {
    assert_owner(container, container.public_update_container, ctx);

    let caller = make_owner_addr(address::to_string(sender(ctx)));
    let target = make_owner_addr(owner_to_remove);

    let len = vector::length(&container.owners);

    let mut active_count = 0;
    let mut i = 0;
    while (i < len) {
        let owner = vector::borrow(&container.owners, i);
        if (!owner.removed) {
            active_count = active_count + 1;
        };
        i = i + 1;
    };
    assert!(active_count > 1, E_CANNOT_REMOVE_LAST_OWNER);

    let next_index = add_with_wrap(container.last_owner_index, 1);
    container.last_owner_index = next_index;

    i = 0;
    let mut found = false;

    while (i < len) {
        let owner = vector::borrow_mut(&mut container.owners, i);
        if (string_eq(&owner.addr, &target)) {
            assert!(!string_eq(&caller, &target), E_CANNOT_REMOVE_SELF);
            owner.removed = true;
            owner.sequence_index = next_index;
            found = true;
            break;
        };
        i = i + 1;
    };

    assert!(found, E_OWNER_NOT_FOUND);

    event::emit(OwnerRemovedEvent {
        container_id: object::id(container),
        owner_addr: target,
        sequence_index: next_index,
    });
}

/// ==========================
/// CHILD CONTAINERS
/// ==========================
public entry fun attach_container_child(
    parent_container: &mut Container,
    child_container: &mut Container,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    ctx: &mut TxContext,
) {
    // Check ownership of both containers
    assert_owner(parent_container, parent_container.public_attach_container_child, ctx);
    assert_owner(child_container, child_container.public_attach_container_child, ctx);

    // Ensure parent and child are not the same
    assert!(object::id(parent_container) != object::id(child_container), 200);

    // Increment child index with wrapping
    let next_index = add_with_wrap(parent_container.last_child_index, 1);

    let child_container_link = ChildContainerLink {
        id: object::new(ctx),
        parent_container_id: object::id(parent_container),
        child_container_id: object::id(child_container),
        external_id: external_id,
        name: name,
        description: description,
        content: content,
        sequence_index: next_index,
        prev_id: parent_container.last_child_id,
    };

    let child_container_link_id = object::id(&child_container_link);
    parent_container.last_child_id = option::some(child_container_link_id);
    parent_container.last_child_index = next_index;
    child_container.sequence_index = next_index;

    // Emit full snapshot event
    event::emit(ChildContainerLinkAttachedEvent {
        id: object::id(&child_container_link),
        parent_container_id: object::id(parent_container),
        child_container_id: object::id(child_container),
        external_id: child_container_link.external_id,
        name: child_container_link.name,
        description: child_container_link.description,
        content: child_container_link.content,
        sequence_index: child_container_link.sequence_index,
        prev_id: child_container_link.prev_id,
    });

    transfer::share_object(child_container_link);
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
    ctx: &mut TxContext,
) {
    let owner_addr = make_owner_addr(address::to_string(sender(ctx)));

    // Create owner object
    let owner = Owner {
        id: object::new(ctx),
        addr: owner_addr,
        role: string::utf8(b"creator"),
        removed: false,
        sequence_index: 0,
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
        sequence_index: 0,
        public_update_container,
        public_attach_container_child,
        public_create_data_type,
        public_publish_data_item,
        last_owner_id: option::some(owner_id),
        last_child_id: option::none(),
        last_data_type_id: option::none(),
        last_data_item_id: option::none(),
        last_owner_index: 0,
        last_child_index: 0,
        last_data_type_index: 0,
        last_data_item_index: 0,
    };

    let container_id = object::id(&container);

    // --- Emit full snapshot event ---
    let mut owner_addrs = vector::empty<string::String>();
    let len = vector::length(&container.owners);
    let mut i = 0;
    while (i < len) {
        let owner = vector::borrow(&container.owners, i);
        vector::push_back(&mut owner_addrs, owner.addr);
        i = i + 1;
    };

    event::emit(ContainerCreatedEvent {
        object_id: container_id,
        external_id: container.external_id,
        owners: owner_addrs,
        name: container.name,
        description: container.description,
        content: container.content,
        sequence_index: container.sequence_index,
        public_update_container: container.public_update_container,
        public_attach_container_child: container.public_attach_container_child,
        public_create_data_type: container.public_create_data_type,
        public_publish_data_item: container.public_publish_data_item,
        last_owner_id: container.last_owner_id,
        last_child_id: container.last_child_id,
        last_data_type_id: container.last_data_type_id,
        last_data_item_id: container.last_data_item_id,
        last_owner_index: container.last_owner_index,
        last_child_index: container.last_child_index,
        last_data_type_index: container.last_data_type_index,
        last_data_item_index: container.last_data_item_index,
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
    tag_index: u128,
    ctx: &mut TxContext,
) {
    assert_owner(container, container.public_create_data_type, ctx);

    let next_index = add_with_wrap(container.last_data_type_index, 1);

    let data_type = DataType {
        id: object::new(ctx),
        container_id: object::id(container),
        external_id,
        name,
        description,
        content: content,
        schemas: schemas,
        sequence_index: next_index,
        tag_index: tag_index,
        last_data_item_id: option::none(),
        prev_id: container.last_data_type_id,
    };
    
    let data_type_id = object::id(&data_type);
    container.last_data_type_id = option::some(data_type_id);
    container.last_data_type_index = next_index;

    // Emit full snapshot
    event::emit(DataTypeCreatedEvent {
        object_id: data_type_id,
        container_id: object::id(container),
        external_id: data_type.external_id,
        name: data_type.name,
        description: data_type.description,
        content: data_type.content,
        schemas: data_type.schemas,
        sequence_index: data_type.sequence_index,
        tag_index: data_type.tag_index,
        last_data_item_id: data_type.last_data_item_id,
        prev_id: data_type.prev_id,
    });

    transfer::share_object(data_type);
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
    tag_index: u128,
    ctx: &mut TxContext,
) {
    assert_owner(container, container.public_publish_data_item, ctx);
    assert!(data_type.container_id == object::id(container), E_INVALID_DATATYPE);

    let next_index = add_with_wrap(container.last_data_item_index, 1);

    let creator = address::to_string(sender(ctx));

    let data_item = DataItem {
        id: object::new(ctx),
        container_id: object::id(container),
        data_type_id: object::id(data_type),
        external_id,
        creator,
        name,
        description,
        content,
        sequence_index: next_index,
        tag_index,
        prev_id: container.last_data_item_id,
        prev_data_type_item_id: data_type.last_data_item_id,
    };

    let data_item_id = object::id(&data_item);
    data_type.last_data_item_id = option::some(data_item_id);
    container.last_data_item_id = option::some(data_item_id);
    container.last_data_item_index = next_index;

    // Emit full snapshot
    event::emit(DataItemPublishedEvent {
        object_id: data_item_id,
        container_id: object::id(container),
        data_type_id: object::id(data_type),
        external_id: data_item.external_id,
        creator: data_item.creator,
        name: data_item.name,
        description: data_item.description,
        content: data_item.content,
        sequence_index: data_item.sequence_index,
        tag_index: data_item.tag_index,
        prev_id: data_item.prev_id,
        prev_data_type_item_id: data_item.prev_data_type_item_id,
    });

    transfer::share_object(data_item);
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
    ctx: &mut TxContext,
) {
    assert_owner(container, container.public_update_container, ctx);

    container.name = new_name;
    container.description = new_description;
    container.content = new_content;
    container.external_id = new_external_id;

    event::emit(ContainerUpdatedEvent {
        object_id: object::id(container),
        external_id: container.external_id,
        name: container.name,
        description: container.description,
        content: container.content,
        sequence_index: container.sequence_index,
        public_update_container: container.public_update_container,
        public_attach_container_child: container.public_attach_container_child,
        public_create_data_type: container.public_create_data_type,
        public_publish_data_item: container.public_publish_data_item,
    });
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
    new_tag_index: u128,
    ctx: &mut TxContext,
) {
    assert_owner(container, container.public_create_data_type, ctx);
    assert!(data_type.container_id == object::id(container), E_INVALID_DATATYPE);

    data_type.external_id = new_external_id;
    data_type.name = new_name;
    data_type.description = new_description;
    data_type.content = new_content;
    data_type.schemas = new_schemas;
    data_type.tag_index = new_tag_index;

    event::emit(DataTypeUpdatedEvent {
        object_id: object::id(data_type),
        container_id: data_type.container_id,
        external_id: data_type.external_id,
        name: data_type.name,
        description: data_type.description,
        content: data_type.content,
        schemas: data_type.schemas,
        sequence_index: data_type.sequence_index,
        tag_index: data_type.tag_index,
    });
}
