module sendit_messenger::generic_store;

use iota::address;
use iota::event;
use iota::object::{Self, UID, ID};
use iota::transfer;
use iota::tx_context::{TxContext, sender};
use std::string;
use std::vector;

/// true max 340282366920938463463374607431768211455
const MAX_u128: u128 = 340282366920938463463374607431768211450;

const E_NOT_OWNER: u64 = 0x1000;
const E_INVALID_DATATYPE: u64 = 1001;
const E_CANNOT_REMOVE_LAST_OWNER: u64 = 1002;
const E_CANNOT_REMOVE_SELF: u64 = 1003;
const E_OWNER_NOT_FOUND: u64 = 1004;

// ==========================
// OWNERS
// ==========================
public struct Owner has key, store {
    id: UID,
    addr: string::String,
    role: string::String,
    removed: bool,
    sequence_index: u128,
    prev_id: Option<ID>,
}

// ==========================
// OBJECTS
// ==========================
public struct ContainerChain has key, store {
    id: UID,
    sequence_index_counter: u128,
    last_container_id: Option<ID>,
}

public struct Container has key, store {
    id: UID,
    external_id: string::String,
    owners: vector<Owner>,
    owners_active: u32,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
    last_owner_id: Option<ID>,
    last_container_child_id: Option<ID>,
    last_data_type_id: Option<ID>,
    last_data_item_id: Option<ID>,
    last_owner_index: u128,
    last_container_child_index: u128,
    last_data_type_index: u128,
    last_data_item_index: u128,
    event_create: bool,
    event_publish: bool,
    event_attach: bool,
    event_add: bool,
    event_remove: bool,
    event_update: bool,
    prev_id: Option<ID>,
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
    external_index: u128,
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
    external_index: u128,
    prev_id: Option<ID>,
    prev_data_type_item_id: Option<ID>,
}

public struct ContainerChildLink has key, store {
    id: UID,
    container_parent_id: ID,
    container_child_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    prev_id: Option<ID>,
}

// ==========================
// EVENTS (FULL OBJECT SNAPSHOT)
// ==========================
public struct ContainerCreatedEvent has copy, drop {
    object_id: ID,
    external_id: string::String,
    owners: vector<string::String>, // addresses
    owners_active: u32,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
    last_owner_id: Option<ID>,
    last_container_child_id: Option<ID>,
    last_data_type_id: Option<ID>,
    last_data_item_id: Option<ID>,
    last_owner_index: u128,
    last_container_child_index: u128,
    last_data_type_index: u128,
    last_data_item_index: u128,
    event_create: bool,
    event_publish: bool,
    event_attach: bool,
    event_add: bool,
    event_remove: bool,
    event_update: bool,
    prev_id: Option<ID>,
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
    external_index: u128,
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
    external_index: u128,
    prev_id: Option<ID>,
    prev_data_type_item_id: Option<ID>,
}

public struct ContainerChildLinkAttachedEvent has copy, drop {
    id: ID,
    container_parent_id: ID,
    container_child_id: ID,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    prev_id: Option<ID>,
}

public struct OwnerAddedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    addr: string::String,
    role: string::String,
    removed: bool,
    sequence_index: u128,
    prev_id: Option<ID>,
}

public struct OwnerRemovedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    addr: string::String,
    role: string::String,
    removed: bool,
    sequence_index: u128,
    prev_id: Option<ID>,
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
    external_index: u128,
}

fun init(ctx: &mut TxContext) {
    let chain = ContainerChain {
        id: object::new(ctx),
        sequence_index_counter: 0,
        last_container_id: option::none<ID>(),
    };

    transfer::share_object(chain);
}

// ==========================
// STRING HELPERS
// ==========================
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

// ==========================
// AUTHORIZATION HELPERS
// ==========================
fun assert_owner(container: &Container, asserted: bool, ctx: &TxContext) {
    if (!asserted) {
        let caller_addr = make_owner_addr(address::to_string(sender(ctx)));
        let len = vector::length(&container.owners);
        let mut i = 0;

        while (i < len) {
            let owner = vector::borrow(&container.owners, i);
            if (!owner.removed && string_eq(&owner.addr, &caller_addr)) {
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

// ==========================
// CONTAINER OWNER MANAGEMENT
// ==========================
public entry fun add_owner(
    container: &mut Container,
    new_owner: string::String,
    role: string::String,
    ctx: &mut TxContext,
) {
    assert_owner(container, container.public_update_container, ctx);

    let owner_addr = make_owner_addr(new_owner);
    let container_id = object::id(container);

    let len = vector::length(&container.owners);
    let mut i = 0;
    let mut found = false;

    while (i < len) {
        let owner = vector::borrow_mut(&mut container.owners, i);
        if (string_eq(&owner.addr, &owner_addr)) {
            let was_removed = owner.removed;
            if (was_removed) {
                owner.removed = false;
                container.owners_active = container.owners_active + 1;
            };

            owner.role = role;
            found = true;

            if (container.event_add && was_removed) {
                event::emit(OwnerAddedEvent {
                    object_id: object::id(owner),
                    container_id: container_id,
                    addr: owner_addr,
                    role: owner.role,
                    removed: owner.removed,
                    sequence_index: owner.sequence_index,
                    prev_id: owner.prev_id,
                });
            };

            break;
        };
        i = i + 1;
    };

    if (!found) {
        let next_index = add_with_wrap(container.last_owner_index, 1);
        container.last_owner_index = next_index;

        let owner = Owner {
            id: object::new(ctx),
            addr: owner_addr,
            role,
            removed: false,
            sequence_index: next_index,
            prev_id: container.last_owner_id,
        };

        let owner_id = object::id(&owner);
        let owner_removed = owner.removed;
        let owner_prev_id = owner.prev_id;
        container.last_owner_id = option::some(owner_id);
        container.owners_active = container.owners_active + 1;

        vector::push_back(
            &mut container.owners,
            owner,
        );

        if (container.event_add) {
            event::emit(OwnerAddedEvent {
                object_id: owner_id,
                container_id: container_id,
                addr: owner_addr,
                role: role,
                removed: owner_removed,
                sequence_index: next_index,
                prev_id: owner_prev_id,
            });
        };
    };
}

public entry fun remove_owner(
    container: &mut Container,
    owner_addr_remove: string::String,
    ctx: &TxContext,
) {
    assert_owner(container, container.public_update_container, ctx);
    assert!(container.owners_active > 1, E_CANNOT_REMOVE_LAST_OWNER);

    let container_id = object::id(container);
    let caller_addr = make_owner_addr(address::to_string(sender(ctx)));
    let owner_addr = make_owner_addr(owner_addr_remove);
    let len = vector::length(&container.owners);

    let mut i = 0;
    let mut found = false;
    while (i < len) {
        let owner = vector::borrow_mut(&mut container.owners, i);
        if (string_eq(&owner.addr, &owner_addr)) {
            assert!(!string_eq(&caller_addr, &owner_addr), E_CANNOT_REMOVE_SELF);
            if (owner.removed) {
                abort E_OWNER_NOT_FOUND;
            };
            owner.removed = true;
            container.owners_active = container.owners_active - 1;
            found = true;
            if (container.event_remove) {
                event::emit(OwnerRemovedEvent {
                    object_id: object::id(owner),
                    container_id: container_id,
                    addr: owner_addr,
                    role: owner.role,
                    removed: owner.removed,
                    sequence_index: owner.sequence_index,
                    prev_id: owner.prev_id,
                });
            };
            break;
        };
        i = i + 1;
    };

    assert!(found, E_OWNER_NOT_FOUND);
}

// ==========================
// CHILD CONTAINERS
// ==========================
public entry fun attach_container_child(
    container_parent: &mut Container,
    container_child: &mut Container,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    ctx: &mut TxContext,
) {
    // Check ownership of both containers
    assert_owner(container_parent, container_parent.public_attach_container_child, ctx);
    assert_owner(container_child, container_child.public_attach_container_child, ctx);

    // Ensure parent and child are not the same
    assert!(object::id(container_parent) != object::id(container_child), 200);

    // Increment child index with wrapping
    let next_index = add_with_wrap(container_parent.last_container_child_index, 1);

    let container_child_link = ContainerChildLink {
        id: object::new(ctx),
        container_parent_id: object::id(container_parent),
        container_child_id: object::id(container_child),
        external_id: external_id,
        name: name,
        description: description,
        content: content,
        sequence_index: next_index,
        prev_id: container_parent.last_container_child_id,
    };

    let container_child_link_id = object::id(&container_child_link);
    container_parent.last_container_child_id = option::some(container_child_link_id);
    container_parent.last_container_child_index = next_index;
    container_child.sequence_index = next_index;

    // Emit full snapshot event
    if (container_parent.event_attach) {
        event::emit(ContainerChildLinkAttachedEvent {
            id: object::id(&container_child_link),
            container_parent_id: object::id(container_parent),
            container_child_id: object::id(container_child),
            external_id: container_child_link.external_id,
            name: container_child_link.name,
            description: container_child_link.description,
            content: container_child_link.content,
            sequence_index: container_child_link.sequence_index,
            prev_id: container_child_link.prev_id,
        });
    };

    transfer::share_object(container_child_link);
}

// ==========================
// CONTAINER
// ==========================
public entry fun create_container(
    container_chain: &mut ContainerChain,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
    event_create: bool,
    event_publish: bool,
    event_attach: bool,
    event_add: bool,
    event_remove: bool,
    event_update: bool,
    ctx: &mut TxContext,
) {
    let owner_addr = make_owner_addr(address::to_string(sender(ctx)));

    // Create owner object
    let owner = Owner {
        id: object::new(ctx),
        addr: owner_addr,
        role: string::utf8(b"creator"),
        removed: false,
        sequence_index: 1,
        prev_id: option::none(),
    };

    let owner_id = object::id(&owner);

    // Create container
    let container = Container {
        id: object::new(ctx),
        owners: vector::singleton(owner),
        owners_active: 1,
        external_id: external_id,
        name: name,
        description: description,
        content: content,
        sequence_index: 1,
        public_update_container: public_update_container,
        public_attach_container_child: public_attach_container_child,
        public_create_data_type: public_create_data_type,
        public_publish_data_item: public_publish_data_item,
        last_owner_id: option::some(owner_id),
        last_container_child_id: option::none(),
        last_data_type_id: option::none(),
        last_data_item_id: option::none(),
        last_owner_index: 0,
        last_container_child_index: 0,
        last_data_type_index: 0,
        last_data_item_index: 0,
        event_create: event_create,
        event_publish: event_publish,
        event_attach: event_attach,
        event_add: event_add,
        event_remove: event_remove,
        event_update: event_update,
        prev_id: container_chain.last_container_id,
    };

    let container_id = object::id(&container);

    // update container chain
    container_chain.sequence_index_counter = add_with_wrap(container_chain.sequence_index_counter, 1);
    container_chain.last_container_id = option::some(container_id);

    // --- Emit full snapshot event ---
    let mut owner_addrs = vector::empty<string::String>();
    let len = vector::length(&container.owners);
    let mut i = 0;
    while (i < len) {
        let owner = vector::borrow(&container.owners, i);
        vector::push_back(&mut owner_addrs, owner.addr);
        i = i + 1;
    };

    if (container.event_create) {
        event::emit(ContainerCreatedEvent {
            object_id: container_id,
            external_id: container.external_id,
            owners: owner_addrs,
            owners_active: 1,
            name: container.name,
            description: container.description,
            content: container.content,
            sequence_index: container.sequence_index,
            public_update_container: container.public_update_container,
            public_attach_container_child: container.public_attach_container_child,
            public_create_data_type: container.public_create_data_type,
            public_publish_data_item: container.public_publish_data_item,
            last_owner_id: container.last_owner_id,
            last_container_child_id: container.last_container_child_id,
            last_data_type_id: container.last_data_type_id,
            last_data_item_id: container.last_data_item_id,
            last_owner_index: container.last_owner_index,
            last_container_child_index: container.last_container_child_index,
            last_data_type_index: container.last_data_type_index,
            last_data_item_index: container.last_data_item_index,
            event_create: container.event_create,
            event_publish: container.event_publish,
            event_attach: container.event_attach,
            event_add: container.event_add,
            event_remove: container.event_remove,
            event_update: container.event_update,
            prev_id: container.prev_id,
        });
    };

    transfer::share_object(container);
}

// ==========================
// DATA TYPE
// ==========================
public entry fun create_data_type(
    container: &mut Container,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    schemas: string::String,
    external_index: u128,
    ctx: &mut TxContext,
) {
    assert_owner(container, container.public_create_data_type, ctx);

    let next_index = add_with_wrap(container.last_data_type_index, 1);

    let data_type = DataType {
        id: object::new(ctx),
        container_id: object::id(container),
        external_id: external_id,
        name: name,
        description: description,
        content: content,
        schemas: schemas,
        sequence_index: next_index,
        external_index: external_index,
        last_data_item_id: option::none(),
        prev_id: container.last_data_type_id,
    };

    let data_type_id = object::id(&data_type);
    container.last_data_type_id = option::some(data_type_id);
    container.last_data_type_index = next_index;

    // Emit full snapshot
    if (container.event_create) {
        event::emit(DataTypeCreatedEvent {
            object_id: data_type_id,
            container_id: object::id(container),
            external_id: data_type.external_id,
            name: data_type.name,
            description: data_type.description,
            content: data_type.content,
            schemas: data_type.schemas,
            sequence_index: data_type.sequence_index,
            external_index: data_type.external_index,
            last_data_item_id: data_type.last_data_item_id,
            prev_id: data_type.prev_id,
        });
    };

    transfer::share_object(data_type);
}

// ==========================
// DATA ITEM
// ==========================
public entry fun publish_data_item(
    container: &mut Container,
    data_type: &mut DataType,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    external_index: u128,
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
        external_id: external_id,
        creator: creator,
        name: name,
        description: description,
        content: content,
        sequence_index: next_index,
        external_index: external_index,
        prev_id: container.last_data_item_id,
        prev_data_type_item_id: data_type.last_data_item_id,
    };

    let data_item_id = object::id(&data_item);
    data_type.last_data_item_id = option::some(data_item_id);
    container.last_data_item_id = option::some(data_item_id);
    container.last_data_item_index = next_index;

    // Emit full snapshot
    if (container.event_publish) {
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
            external_index: data_item.external_index,
            prev_id: data_item.prev_id,
            prev_data_type_item_id: data_item.prev_data_type_item_id,
        });
    };

    transfer::share_object(data_item);
}

// ==========================
// UPDATE METHODS
// ==========================

// Update container
public entry fun update_container(
    container: &mut Container,
    new_external_id: string::String,
    new_name: string::String,
    new_description: string::String,
    new_content: string::String,
    ctx: &mut TxContext,
) {
    assert_owner(container, container.public_update_container, ctx);

    container.external_id = new_external_id;
    container.name = new_name;
    container.description = new_description;
    container.content = new_content;

    if (container.event_update) {
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
    };
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
    new_external_index: u128,
    ctx: &mut TxContext,
) {
    assert_owner(container, container.public_create_data_type, ctx);
    assert!(data_type.container_id == object::id(container), E_INVALID_DATATYPE);

    data_type.external_id = new_external_id;
    data_type.name = new_name;
    data_type.description = new_description;
    data_type.content = new_content;
    data_type.schemas = new_schemas;
    data_type.external_index = new_external_index;

    if (container.event_update) {
        event::emit(DataTypeUpdatedEvent {
            object_id: object::id(data_type),
            container_id: data_type.container_id,
            external_id: data_type.external_id,
            name: data_type.name,
            description: data_type.description,
            content: data_type.content,
            schemas: data_type.schemas,
            sequence_index: data_type.sequence_index,
            external_index: data_type.external_index,
        });
    };
}
