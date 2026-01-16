module sendit_messenger::generic_store;

use iota::address;
use iota::clock::Clock;
use iota::event;
use iota::object::{Self, UID, ID};
use iota::transfer;
use iota::tx_context::{TxContext, sender};
use std::string;
use std::vector;

// ==========================
// CONSTANTS
// ==========================
/// true max 340282366920938463463374607431768211455
const MAX_u128: u128 = 340282366920938463463374607431768211450;
// error constants
const E_NOT_OWNER: u64 = 0x1000;
const E_INVALID_DATATYPE: u64 = 1001;
const E_CANNOT_REMOVE_LAST_OWNER: u64 = 1002;
const E_CANNOT_REMOVE_SELF: u64 = 1003;
const E_OWNER_NOT_FOUND: u64 = 1004;
const E_NO_ACTIVE_OWNERS: u64 = 1005;
const E_INVALID_CONTAINER: u64 = 1006;

// ==========================
// CHAINS
// ==========================
public struct ContainerChain has key, store {
    id: UID,
    sequence_index_counter: u128,
    last_container_id: Option<ID>,
}

// ==========================
// COMMONS
// ==========================
public struct Creator has store {
    creator_addr: string::String,
    creator_update_addr: string::String,
    creator_timestamp_ms: u64,
    creator_update_timestamp_ms: u64,
}

public struct Specification has store {
    version: string::String,
    schemas: string::String,
    apis: string::String,
    resources: string::String,
}

public struct ContainerPermission has store {
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
}

public struct ContainerEventConfig has store {
    event_create: bool,
    event_publish: bool,
    event_attach: bool,
    event_add: bool,
    event_remove: bool,
    event_update: bool,
}

public struct CreatorEvent has copy, drop {
    creator_addr: string::String,
    creator_update_addr: string::String,
    creator_timestamp_ms: u64,
    creator_update_timestamp_ms: u64,
}

public struct SpecificationEvent has copy, drop {
    version: string::String,
    schemas: string::String,
    apis: string::String,
    resources: string::String,
}

public struct ContainerPermissionEvent has copy, drop {
    public_update_container: bool,
    public_attach_container_child: bool,
    public_create_data_type: bool,
    public_publish_data_item: bool,
}

public struct ContainerEventConfigEvent has copy, drop {
    event_create: bool,
    event_publish: bool,
    event_attach: bool,
    event_add: bool,
    event_remove: bool,
    event_update: bool,
}

// ==========================
// OBJECTS
// ==========================
public struct Container has key, store {
    id: UID,
    external_id: string::String,
    creator: Creator,
    owners: vector<Owner>,
    owners_active_count: u32,
    name: string::String,
    description: string::String,
    content: string::String,
    specification: Specification,
    permission: ContainerPermission,
    event_config: ContainerEventConfig,
    sequence_index: u128,
    external_index: u128,
    last_owner_index: u128,
    last_container_child_index: u128,
    last_data_type_index: u128,
    last_data_item_index: u128,
    last_owner_id: Option<ID>,
    last_container_child_id: Option<ID>,
    last_data_type_id: Option<ID>,
    last_data_item_id: Option<ID>,
    prev_id: Option<ID>,
}

public struct DataType has key, store {
    id: UID,
    container_id: ID,
    external_id: string::String,
    creator: Creator,
    name: string::String,
    description: string::String,
    content: string::String,
    specification: Specification,
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
    creator: Creator,
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
    creator: Creator,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    external_index: u128,
    prev_id: Option<ID>,
}

public struct Owner has key, store {
    id: UID,
    creator: Creator,
    addr: string::String,
    role: string::String,
    removed: bool,
    sequence_index: u128,
    prev_id: Option<ID>,
}

// ==========================
// EVENTS (FULL OBJECT SNAPSHOT)
// ==========================
public struct ContainerCreatedEvent has copy, drop {
    object_id: ID,
    external_id: string::String,
    creator: CreatorEvent,
    owners: vector<string::String>, // addresses
    owners_active_count: u32,
    name: string::String,
    description: string::String,
    content: string::String,
    specification: SpecificationEvent,
    permission: ContainerPermissionEvent,
    event_config: ContainerEventConfigEvent,
    sequence_index: u128,
    external_index: u128,
    last_owner_index: u128,
    last_container_child_index: u128,
    last_data_type_index: u128,
    last_data_item_index: u128,
    last_owner_id: Option<ID>,
    last_container_child_id: Option<ID>,
    last_data_type_id: Option<ID>,
    last_data_item_id: Option<ID>,
    prev_id: Option<ID>,
}

public struct DataTypeCreatedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    external_id: string::String,
    creator: CreatorEvent,
    name: string::String,
    description: string::String,
    content: string::String,
    specification: SpecificationEvent,
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
    creator: CreatorEvent,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    external_index: u128,
    prev_id: Option<ID>,
    prev_data_type_item_id: Option<ID>,
}

public struct ContainerChildLinkAttachedEvent has copy, drop {
    object_id: ID,
    container_parent_id: ID,
    container_child_id: ID,
    external_id: string::String,
    creator: CreatorEvent,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    external_index: u128,
    prev_id: Option<ID>,
}

public struct OwnerAddedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    creator: CreatorEvent,
    addr: string::String,
    role: string::String,
    removed: bool,
    sequence_index: u128,
    prev_id: Option<ID>,
}

public struct OwnerRemovedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    creator: CreatorEvent,
    addr: string::String,
    role: string::String,
    removed: bool,
    sequence_index: u128,
    prev_id: Option<ID>,
}

public struct ContainerUpdatedEvent has copy, drop {
    object_id: ID,
    external_id: string::String,
    creator: CreatorEvent,
    name: string::String,
    description: string::String,
    content: string::String,
    specification: SpecificationEvent,
    sequence_index: u128,
    external_index: u128,
}

public struct DataTypeUpdatedEvent has copy, drop {
    object_id: ID,
    container_id: ID,
    external_id: string::String,
    creator: CreatorEvent,
    name: string::String,
    description: string::String,
    content: string::String,
    specification: SpecificationEvent,
    sequence_index: u128,
    external_index: u128,
}

public struct ContainerLinkUpdatedEvent has copy, drop {
    object_id: ID,
    container_parent_id: ID,
    container_child_id: ID,
    external_id: string::String,
    creator: CreatorEvent,
    name: string::String,
    description: string::String,
    content: string::String,
    sequence_index: u128,
    external_index: u128,
}

// ==========================
// INIT
// ==========================
fun init(ctx: &mut TxContext) {
    let chain = ContainerChain {
        id: object::new(ctx),
        sequence_index_counter: 0,
        last_container_id: option::none<ID>(),
    };

    transfer::share_object(chain);
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
    version: string::String,
    schemas: string::String,
    apis: string::String,
    resources: string::String,
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
    external_index: u128,
    clock: &Clock,
    ctx: &mut TxContext,
) {
    // Creator info
    let owner_addr = make_owner_addr(address::to_string(sender(ctx)));
    let creator_addr = address::to_string(sender(ctx));
    let creator_timestamp_ms = clock.timestamp_ms();

    let creator_owner = Creator {
        creator_addr: creator_addr,
        creator_update_addr: creator_addr,
        creator_timestamp_ms: creator_timestamp_ms,
        creator_update_timestamp_ms: creator_timestamp_ms,
    };

    // Owner object
    let owner = Owner {
        id: object::new(ctx),
        creator: creator_owner,
        addr: owner_addr,
        role: string::utf8(b"creator"),
        removed: false,
        sequence_index: 1,
        prev_id: option::none(),
    };

    let owner_id = object::id(&owner);

    // Container specification
    let specification = Specification {
        version: version,
        schemas: schemas,
        apis: apis,
        resources: resources,
    };

    let container_permission = ContainerPermission {
        public_update_container: public_update_container,
        public_attach_container_child: public_attach_container_child,
        public_create_data_type: public_create_data_type,
        public_publish_data_item: public_publish_data_item,
    };

    let container_event_config = ContainerEventConfig {
        event_create: event_create,
        event_publish: event_publish,
        event_attach: event_attach,
        event_add: event_add,
        event_remove: event_remove,
        event_update: event_update,
    };

    let creator_container = Creator {
        creator_addr: creator_addr,
        creator_update_addr: creator_addr,
        creator_timestamp_ms: creator_timestamp_ms,
        creator_update_timestamp_ms: creator_timestamp_ms,
    };

    // Container object
    let container = Container {
        id: object::new(ctx),
        owners: vector::singleton(owner),
        owners_active_count: 1,
        external_id: external_id,
        creator: creator_container,
        name: name,
        description: description,
        content: content,
        specification: specification,
        permission: container_permission,
        event_config: container_event_config,
        sequence_index: 1,
        external_index: external_index,
        last_owner_index: 0,
        last_container_child_index: 0,
        last_data_type_index: 0,
        last_data_item_index: 0,
        last_owner_id: option::some(owner_id),
        last_container_child_id: option::none(),
        last_data_type_id: option::none(),
        last_data_item_id: option::none(),
        prev_id: container_chain.last_container_id,
    };

    let container_id = object::id(&container);

    // Update chain
    container_chain.sequence_index_counter =
        add_with_wrap(container_chain.sequence_index_counter, 1);
    container_chain.last_container_id = option::some(container_id);

    // Collect active owner addresses
    let mut owner_addrs = vector::empty<string::String>();
    let len = vector::length(&container.owners);
    let mut i = 0;
    while (i < len) {
        let owner_ref = vector::borrow(&container.owners, i);
        if (!owner_ref.removed) {
            vector::push_back(&mut owner_addrs, owner_ref.addr);
        };
        i = i + 1;
    };

    // Emit event
    if (event_create) {
        let specification_event = SpecificationEvent {
            version: version,
            schemas: schemas,
            apis: apis,
            resources: resources,
        };

        let container_permission_event = ContainerPermissionEvent {
            public_update_container: public_update_container,
            public_attach_container_child: public_attach_container_child,
            public_create_data_type: public_create_data_type,
            public_publish_data_item: public_publish_data_item,
        };

        let container_event_config_event = ContainerEventConfigEvent {
            event_create: event_create,
            event_publish: event_publish,
            event_attach: event_attach,
            event_add: event_add,
            event_remove: event_remove,
            event_update: event_update,
        };

        let creator_event = CreatorEvent {
            creator_addr: creator_addr,
            creator_update_addr: creator_addr,
            creator_timestamp_ms: creator_timestamp_ms,
            creator_update_timestamp_ms: creator_timestamp_ms,
        };

        event::emit(ContainerCreatedEvent {
            object_id: container_id,
            external_id: container.external_id,
            creator: creator_event,
            owners: owner_addrs,
            owners_active_count: 1,
            name: container.name,
            description: container.description,
            content: container.content,
            specification: specification_event,
            permission: container_permission_event,
            event_config: container_event_config_event,
            sequence_index: container.sequence_index,
            external_index: container.external_index,
            last_owner_index: container.last_owner_index,
            last_container_child_index: container.last_container_child_index,
            last_data_type_index: container.last_data_type_index,
            last_data_item_index: container.last_data_item_index,
            last_owner_id: container.last_owner_id,
            last_container_child_id: container.last_container_child_id,
            last_data_type_id: container.last_data_type_id,
            last_data_item_id: container.last_data_item_id,
            prev_id: container.prev_id,
        });
    };

    if (event_add) {
        let creator_owner_event = CreatorEvent {
            creator_addr: creator_addr,
            creator_update_addr: creator_addr,
            creator_timestamp_ms: creator_timestamp_ms,
            creator_update_timestamp_ms: creator_timestamp_ms,
        };

        event::emit(OwnerAddedEvent {
            object_id: owner_id,
            container_id: container_id,
            creator: creator_owner_event,
            addr: owner_addr,
            role: string::utf8(b"creator"),
            removed: false,
            sequence_index: 1,
            prev_id: option::none(),
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
    version: string::String,
    schemas: string::String,
    apis: string::String,
    resources: string::String,
    external_index: u128,
    clock: &Clock,
    ctx: &mut TxContext,
) {
    let permission_ref = &container.permission;
    let event_config_ref = &container.event_config;
    assert_owner(container, permission_ref.public_create_data_type, ctx);

    let next_index = add_with_wrap(container.last_data_type_index, 1);
    let creator_addr = address::to_string(sender(ctx));
    let creator_timestamp_ms = clock.timestamp_ms();

    let creator = Creator {
        creator_addr: creator_addr,
        creator_update_addr: creator_addr,
        creator_timestamp_ms: creator_timestamp_ms,
        creator_update_timestamp_ms: creator_timestamp_ms,
    };

    let specification = Specification {
        version: version,
        schemas: schemas,
        apis: apis,
        resources: resources,
    };

    let data_type = DataType {
        id: object::new(ctx),
        container_id: object::id(container),
        external_id: external_id,
        creator: creator,
        name: name,
        description: description,
        content: content,
        specification: specification,
        sequence_index: next_index,
        external_index: external_index,
        last_data_item_id: option::none(),
        prev_id: container.last_data_type_id,
    };

    let data_type_id = object::id(&data_type);
    container.last_data_type_id = option::some(data_type_id);
    container.last_data_type_index = next_index;

    if (event_config_ref.event_create) {
        let specification_event = SpecificationEvent {
            version: version,
            schemas: schemas,
            apis: apis,
            resources: resources,
        };

        let creator_event = CreatorEvent {
            creator_addr: creator_addr,
            creator_update_addr: creator_addr,
            creator_timestamp_ms: creator_timestamp_ms,
            creator_update_timestamp_ms: creator_timestamp_ms,
        };

        event::emit(DataTypeCreatedEvent {
            object_id: data_type_id,
            container_id: object::id(container),
            external_id: data_type.external_id,
            creator: creator_event,
            name: data_type.name,
            description: data_type.description,
            content: data_type.content,
            specification: specification_event,
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
    clock: &Clock,
    ctx: &mut TxContext,
) {
    let permission_ref = &container.permission;
    let event_config_ref = &container.event_config;
    assert_owner(container, permission_ref.public_publish_data_item, ctx);
    assert!(data_type.container_id == object::id(container), E_INVALID_DATATYPE);

    let next_index = add_with_wrap(container.last_data_item_index, 1);
    let creator_addr = address::to_string(sender(ctx));
    let creator_timestamp_ms = clock.timestamp_ms();

    let creator = Creator {
        creator_addr: creator_addr,
        creator_update_addr: creator_addr,
        creator_timestamp_ms: creator_timestamp_ms,
        creator_update_timestamp_ms: creator_timestamp_ms,
    };

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

    if (event_config_ref.event_publish) {
        let creator_event = CreatorEvent {
            creator_addr: creator_addr,
            creator_update_addr: creator_addr,
            creator_timestamp_ms: creator_timestamp_ms,
            creator_update_timestamp_ms: creator_timestamp_ms,
        };

        event::emit(DataItemPublishedEvent {
            object_id: data_item_id,
            container_id: object::id(container),
            data_type_id: object::id(data_type),
            external_id: data_item.external_id,
            creator: creator_event,
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
// CHILD CONTAINERS
// ==========================
public entry fun attach_container_child(
    container_parent: &mut Container,
    container_child: &mut Container,
    external_id: string::String,
    name: string::String,
    description: string::String,
    content: string::String,
    external_index: u128,
    clock: &Clock,
    ctx: &mut TxContext,
) {
    // Check ownership of both containers
    let parent_permission_ref = &container_parent.permission;
    let child_permission_ref = &container_child.permission;
    let parent_event_config_ref = &container_parent.event_config;
    assert_owner(container_parent, parent_permission_ref.public_attach_container_child, ctx);
    assert_owner(container_child, child_permission_ref.public_attach_container_child, ctx);

    // Ensure parent and child are not the same
    assert!(object::id(container_parent) != object::id(container_child), E_INVALID_CONTAINER);

    // Increment sequence
    let next_index = add_with_wrap(container_parent.last_container_child_index, 1);

    // Who is creating this link
    let creator_addr = address::to_string(sender(ctx));
    let creator_timestamp_ms = clock.timestamp_ms();

    // Creator struct
    let creator = Creator {
        creator_addr: creator_addr,
        creator_update_addr: creator_addr,
        creator_timestamp_ms: creator_timestamp_ms,
        creator_update_timestamp_ms: creator_timestamp_ms,
    };

    // Create the container child link
    let container_child_link = ContainerChildLink {
        id: object::new(ctx),
        container_parent_id: object::id(container_parent),
        container_child_id: object::id(container_child),
        external_id: external_id,
        creator: creator,
        name: name,
        description: description,
        content: content,
        sequence_index: next_index,
        external_index: external_index,
        prev_id: container_parent.last_container_child_id,
    };

    let container_child_link_id = object::id(&container_child_link);
    container_parent.last_container_child_id = option::some(container_child_link_id);
    container_parent.last_container_child_index = next_index;
    container_child.sequence_index = next_index;

    // Emit event if configured
    if (parent_event_config_ref.event_attach) {
        let creator_event = CreatorEvent {
            creator_addr: container_child_link.creator.creator_addr,
            creator_update_addr: container_child_link.creator.creator_update_addr,
            creator_timestamp_ms: container_child_link.creator.creator_timestamp_ms,
            creator_update_timestamp_ms: container_child_link.creator.creator_update_timestamp_ms,
        };

        event::emit(ContainerChildLinkAttachedEvent {
            object_id: container_child_link_id,
            container_parent_id: object::id(container_parent),
            container_child_id: object::id(container_child),
            external_id: container_child_link.external_id,
            creator: creator_event,
            name: container_child_link.name,
            description: container_child_link.description,
            content: container_child_link.content,
            sequence_index: container_child_link.sequence_index,
            external_index: container_child_link.external_index,
            prev_id: container_child_link.prev_id,
        });
    };

    // Share object to allow other modules access
    transfer::share_object(container_child_link);
}

// ==========================
// CONTAINER OWNER MANAGEMENT
// ==========================
public entry fun add_owner(
    container: &mut Container,
    new_owner: string::String,
    role: string::String,
    clock: &Clock,
    ctx: &mut TxContext,
) {
    let permission_ref = &container.permission;
    let event_config_ref = &container.event_config;
    assert_owner(container, permission_ref.public_update_container, ctx);

    let owner_addr = make_owner_addr(new_owner);
    let container_id = object::id(container);
    let updater_addr = address::to_string(sender(ctx));
    let timestamp_ms = clock.timestamp_ms();

    let len = vector::length(&container.owners);
    let mut i = 0;
    let mut found = false;

    while (i < len) {
        let owner = vector::borrow_mut(&mut container.owners, i);

        if (string_eq(&owner.addr, &owner_addr)) {
            // Owner exists — maybe re-activate if removed
            let was_removed = owner.removed;
            if (was_removed) {
                owner.removed = false;
                container.owners_active_count = container.owners_active_count + 1;
            };

            // Update role and creator metadata
            owner.role = role;
            owner.creator.creator_update_addr = updater_addr;
            owner.creator.creator_update_timestamp_ms = timestamp_ms;

            found = true;

            // Emit event only if owner was previously removed
            if (event_config_ref.event_add && was_removed) {
                let creator_event = CreatorEvent {
                    creator_addr: owner.creator.creator_addr,
                    creator_update_addr: owner.creator.creator_update_addr,
                    creator_timestamp_ms: owner.creator.creator_timestamp_ms,
                    creator_update_timestamp_ms: owner.creator.creator_update_timestamp_ms,
                };

                event::emit(OwnerAddedEvent {
                    object_id: object::id(owner),
                    container_id: container_id,
                    creator: creator_event,
                    addr: owner.addr,
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
        // New owner
        let next_index = add_with_wrap(container.last_owner_index, 1);
        container.last_owner_index = next_index;

        let creator = Creator {
            creator_addr: updater_addr,
            creator_update_addr: updater_addr,
            creator_timestamp_ms: timestamp_ms,
            creator_update_timestamp_ms: timestamp_ms,
        };

        let owner = Owner {
            id: object::new(ctx),
            creator: creator,
            addr: owner_addr,
            role: role,
            removed: false,
            sequence_index: next_index,
            prev_id: container.last_owner_id,
        };

        let owner_id = object::id(&owner);
        container.last_owner_id = option::some(owner_id);
        container.owners_active_count = container.owners_active_count + 1;

        vector::push_back(&mut container.owners, owner);

        if (event_config_ref.event_add) {
            let creator_event = CreatorEvent {
                creator_addr: updater_addr,
                creator_update_addr: updater_addr,
                creator_timestamp_ms: timestamp_ms,
                creator_update_timestamp_ms: timestamp_ms,
            };

            event::emit(OwnerAddedEvent {
                object_id: owner_id,
                container_id: container_id,
                creator: creator_event,
                addr: owner_addr,
                role: role,
                removed: false,
                sequence_index: next_index,
                prev_id: container.last_owner_id, // prev_id before push
            });
        }
    }
}

public entry fun remove_owner(
    container: &mut Container,
    owner_addr_remove: string::String,
    clock: &Clock,
    ctx: &TxContext,
) {
    let permission_ref = &container.permission;
    let event_config_ref = &container.event_config;
    assert_owner(container, permission_ref.public_update_container, ctx);
    assert!(container.owners_active_count > 1, E_CANNOT_REMOVE_LAST_OWNER);

    let container_id = object::id(container);
    let caller_addr = make_owner_addr(address::to_string(sender(ctx)));
    let owner_addr = make_owner_addr(owner_addr_remove);
    let timestamp_ms = clock.timestamp_ms();

    let len = vector::length(&container.owners);
    let mut i = 0;
    let mut found = false;

    while (i < len) {
        let owner = vector::borrow_mut(&mut container.owners, i);

        if (string_eq(&owner.addr, &owner_addr)) {
            // Cannot remove self
            assert!(!string_eq(&caller_addr, &owner_addr), E_CANNOT_REMOVE_SELF);

            if (owner.removed) {
                abort E_OWNER_NOT_FOUND;
            };

            // Mark as removed and update creator metadata
            owner.removed = true;
            owner.creator.creator_update_addr = caller_addr;
            owner.creator.creator_update_timestamp_ms = timestamp_ms;

            container.owners_active_count = container.owners_active_count - 1;
            found = true;

            if (event_config_ref.event_remove) {
                let creator_event = CreatorEvent {
                    creator_addr: owner.creator.creator_addr,
                    creator_update_addr: owner.creator.creator_update_addr,
                    creator_timestamp_ms: owner.creator.creator_timestamp_ms,
                    creator_update_timestamp_ms: owner.creator.creator_update_timestamp_ms,
                };

                event::emit(OwnerRemovedEvent {
                    object_id: object::id(owner),
                    container_id: container_id,
                    creator: creator_event,
                    addr: owner.addr,
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
// UPDATE METHODS
// ==========================
// update container
public entry fun update_container(
    container: &mut Container,
    new_external_id: string::String,
    new_name: string::String,
    new_description: string::String,
    new_content: string::String,
    new_version: string::String,
    new_schemas: string::String,
    new_apis: string::String,
    new_resources: string::String,
    new_external_index: u128,
    clock: &Clock,
    ctx: &mut TxContext,
) {
    let permission_ref = &container.permission;
    let event_config_ref = &container.event_config;
    assert_owner(container, permission_ref.public_update_container, ctx);

    let updater_addr = address::to_string(sender(ctx));
    let updater_timestamp_ms = clock.timestamp_ms();

    // Update creator update fields
    container.creator.creator_update_addr = updater_addr;
    container.creator.creator_update_timestamp_ms = updater_timestamp_ms;

    let spec_ref = &mut container.specification;

    container.external_id = new_external_id;
    container.name = new_name;
    container.description = new_description;
    container.content = new_content;
    spec_ref.version = new_version;
    spec_ref.schemas = new_schemas;
    spec_ref.apis = new_apis;
    spec_ref.resources = new_resources;
    container.external_index = new_external_index;

    if (event_config_ref.event_update) {
        let specification_event = SpecificationEvent {
            version: new_version,
            schemas: new_schemas,
            apis: new_apis,
            resources: new_resources,
        };

        let creator_event = CreatorEvent {
            creator_addr: container.creator.creator_addr,
            creator_update_addr: container.creator.creator_update_addr,
            creator_timestamp_ms: container.creator.creator_timestamp_ms,
            creator_update_timestamp_ms: container.creator.creator_update_timestamp_ms,
        };

        event::emit(ContainerUpdatedEvent {
            object_id: object::id(container),
            external_id: container.external_id,
            creator: creator_event,
            name: container.name,
            description: container.description,
            content: container.content,
            specification: specification_event,
            sequence_index: container.sequence_index,
            external_index: container.external_index,
        });
    };
}

// update data type
public entry fun update_data_type(
    container: &mut Container,
    data_type: &mut DataType,
    new_external_id: string::String,
    new_name: string::String,
    new_description: string::String,
    new_content: string::String,
    new_version: string::String,
    new_schemas: string::String,
    new_apis: string::String,
    new_resources: string::String,
    new_external_index: u128,
    clock: &Clock,
    ctx: &mut TxContext,
) {
    let permission_ref = &container.permission;
    let event_config_ref = &container.event_config;
    assert_owner(container, permission_ref.public_create_data_type, ctx);
    assert!(data_type.container_id == object::id(container), E_INVALID_DATATYPE);

    // Who is performing the update
    let updater_addr = address::to_string(sender(ctx));
    let updater_timestamp_ms = clock.timestamp_ms();

    // Update creator_update fields
    data_type.creator.creator_update_addr = updater_addr;
    data_type.creator.creator_update_timestamp_ms = updater_timestamp_ms;

    let spec_ref = &mut data_type.specification;

    // Update data type fields
    data_type.external_id = new_external_id;
    data_type.name = new_name;
    data_type.description = new_description;
    data_type.content = new_content;
    spec_ref.version = new_version;
    spec_ref.schemas = new_schemas;
    spec_ref.apis = new_apis;
    spec_ref.resources = new_resources;
    data_type.external_index = new_external_index;

    // Emit event including both creator and updater
    if (event_config_ref.event_update) {
        let specification_event = SpecificationEvent {
            version: new_version,
            schemas: new_schemas,
            apis: new_apis,
            resources: new_resources,
        };

        let creator_event = CreatorEvent {
            creator_addr: data_type.creator.creator_addr,
            creator_update_addr: data_type.creator.creator_update_addr,
            creator_timestamp_ms: data_type.creator.creator_timestamp_ms,
            creator_update_timestamp_ms: data_type.creator.creator_update_timestamp_ms,
        };

        event::emit(DataTypeUpdatedEvent {
            object_id: object::id(data_type),
            container_id: data_type.container_id,
            external_id: data_type.external_id,
            creator: creator_event,
            name: data_type.name,
            description: data_type.description,
            content: data_type.content,
            specification: specification_event,
            sequence_index: data_type.sequence_index,
            external_index: data_type.external_index,
        });
    };
}

// update container child link
public entry fun update_container_child_link(
    container_child_link: &mut ContainerChildLink,
    container_parent: &mut Container,
    container_child: &mut Container,
    new_external_id: string::String,
    new_name: string::String,
    new_description: string::String,
    new_content: string::String,
    new_external_index: u128,
    clock: &Clock,
    ctx: &mut TxContext,
) {
    // Defensive invariant — NEVER allow parent == child
    assert!(object::id(container_parent) != object::id(container_child), E_INVALID_CONTAINER);

    let parent_permission_ref = &container_parent.permission;
    let child_permission_ref = &container_child.permission;

    // Same rules as attach
    assert_owner(container_parent, parent_permission_ref.public_attach_container_child, ctx);
    assert_owner(container_child, child_permission_ref.public_attach_container_child, ctx);

    // Who is performing the update
    let updater_addr = address::to_string(sender(ctx));
    let updater_timestamp_ms = clock.timestamp_ms();

    // Update creator_update fields
    container_child_link.creator.creator_update_addr = updater_addr;
    container_child_link.creator.creator_update_timestamp_ms = updater_timestamp_ms;

    // Update the main fields
    container_child_link.external_id = new_external_id;
    container_child_link.name = new_name;
    container_child_link.description = new_description;
    container_child_link.content = new_content;
    container_child_link.external_index = new_external_index;

    // Emit event including both creator and updater
    if (container_parent.event_config.event_update) {
        let creator_event = CreatorEvent {
            creator_addr: container_child_link.creator.creator_addr,
            creator_update_addr: container_child_link.creator.creator_update_addr,
            creator_timestamp_ms: container_child_link.creator.creator_timestamp_ms,
            creator_update_timestamp_ms: container_child_link.creator.creator_update_timestamp_ms,
        };

        event::emit(ContainerLinkUpdatedEvent {
            object_id: object::id(container_child_link),
            container_parent_id: container_child_link.container_parent_id,
            container_child_id: container_child_link.container_child_id,
            external_id: container_child_link.external_id,
            creator: creator_event,
            name: container_child_link.name,
            description: container_child_link.description,
            content: container_child_link.content,
            sequence_index: container_child_link.sequence_index,
            external_index: container_child_link.external_index,
        });
    };
}

// update container owners active count
public entry fun update_container_owners_active_count(container: &mut Container, ctx: &TxContext) {
    let permission_ref = &container.permission;
    assert_owner(container, permission_ref.public_update_container, ctx);

    let len = vector::length(&container.owners);
    let mut i = 0;
    let mut active: u32 = 0;

    while (i < len) {
        let owner = vector::borrow(&container.owners, i);
        if (!owner.removed) {
            active = active + 1;
        };
        i = i + 1;
    };

    // Defensive invariant: there must be at least one active owner
    assert!(active > 0, E_NO_ACTIVE_OWNERS);

    container.owners_active_count = active;
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
                return; // Authorized, exit early
            };
            i = i + 1;
        };

        // If we get here, no owner matched
        abort E_NOT_OWNER;
    }
}

// ==========================
// STRING AND NUMBER HELPERS
// ==========================
fun string_eq(a: &string::String, b: &string::String): bool {
    let ba = string::bytes(a);
    let bb = string::bytes(b);

    if (vector::length(ba) != vector::length(bb)) {
        return false;
    };

    let mut i = 0;
    while (i < vector::length(ba)) {
        if (*vector::borrow(ba, i) != *vector::borrow(bb, i)) {
            return false;
        };
        i = i + 1;
    };

    true
}

fun make_owner_addr(addr: string::String): string::String {
    // Currently just returns the address string; optionally prefix here
    addr
}

public fun add_with_wrap(val: u128, add: u128): u128 {
    if (val > MAX_u128 - add) {
        1
    } else {
        val + add
    }
}
