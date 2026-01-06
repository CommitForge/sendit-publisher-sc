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
        children: vector<ObjectRef>,
        data_types: vector<ObjectRef>,
        data_items: vector<ObjectRef>,
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
        let caller = address::to_string(sender(ctx));
        let len = vector::length(&container.owners);

        let mut i = 0;
        let mut found = false;

        while (i < len) {
            let owner = vector::borrow(&container.owners, i);
            if (string_eq(&owner.addr, &caller)) {
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
            if (string_eq(&owner.addr, addr)) {
                return true;
            };
            i = i + 1;
        };

        false
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
        let owner_str = address::to_string(sender(ctx));

        let owner = Owner {
            id: object::new(ctx),
            addr: owner_str,
            role: string::utf8(b"owner"),
        };

        let container = Container {
            id: object::new(ctx),
            owners: vector::singleton(owner),
            external_id,
            name,
            description,
            children: vector::empty(),
            data_types: vector::empty(),
            data_items: vector::empty(),
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

    public entry fun add_owner(
        container: &mut Container,
        new_owner: string::String,
        role: string::String,
        ctx: &mut TxContext
    ) {
        assert_owner(container, ctx);

        if (!is_owner(container, &new_owner)) {
            vector::push_back(
                &mut container.owners,
                Owner {
                    id: object::new(ctx),
                    addr: new_owner,
                    role,
                }
            );
        };
    }

    /// ==========================
    /// CHILD CONTAINERS
    /// ==========================
    public entry fun attach_container_child(
        parent: &mut Container,
        child: &Container,
        ctx: &TxContext
    ) {
        assert_owner(parent, ctx);

        vector::push_back(
            &mut parent.children,
            ObjectRef {
                object_id: object::id(child),
                external_id: child.external_id,
            }
        );
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

        vector::push_back(
            &mut container.data_types,
            ObjectRef {
                object_id: dt_id,
                external_id: dt.external_id,
            }
        );

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
        };

        let item_id = object::id(&item);

        vector::push_back(
            &mut container.data_items,
            ObjectRef {
                object_id: item_id,
                external_id: item.external_id,
            }
        );

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
