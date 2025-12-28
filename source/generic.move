module sendit_messenger::generic_store {
    use std::vector;
    use std::string::String;
    use iota::object::{Self, UID, ID};
    use iota::tx_context::{TxContext, sender};
    use iota::transfer;
    use iota::event;

    /// ==========================
    /// SHARED LIGHTWEIGHT REF
    /// ==========================
    public struct ObjectRef has copy, drop, store {
        object_id: ID,
        external_id: String,
    }

    /// ==========================
    /// OBJECTS
    /// ==========================
    public struct Container has key, store {
        id: UID,
        owners: vector<address>,
        external_id: String,
        name: String,
        description: String,
        children: vector<ObjectRef>,
        data_types: vector<ObjectRef>,
        data_items: vector<ObjectRef>,
    }

    public struct DataType has key, store {
        id: UID,
        container: ID,
        external_id: String,
        name: String,
        description: String,
    }

    public struct DataItem has key, store {
        id: UID,
        container: ID,
        data_type: ID,
        creator: address,
        external_id: String,
        name: String,
        content: String,
    }

    /// ==========================
    /// EVENTS — CREATION
    /// ==========================
    public struct ContainerCreatedEvent has copy, drop {
        object_id: ID,
        external_id: String,
        owner: address,
        name: String,
        description: String,
    }

    public struct DataTypeCreatedEvent has copy, drop {
        object_id: ID,
        container_id: ID,
        external_id: String,
        name: String,
    }

    public struct DataItemCreatedEvent has copy, drop {
        object_id: ID,
        container_id: ID,
        data_type_id: ID,
        external_id: String,
        creator: address,
        name: String,
    }

    /// ==========================
    /// AUTHORIZATION HELPERS
    /// ==========================
    fun assert_owner(container: &Container, ctx: &TxContext) {
        let caller = sender(ctx);
        let len = vector::length(&container.owners);
        let mut found = false;
        let mut i = 0;
        while (i < len) {
            if (*vector::borrow(&container.owners, i) == caller) {
                found = true;
            };
            i = i + 1;
        };
        assert!(found, 1);
    }

    fun is_owner(container: &Container, addr: address): bool {
        let len = vector::length(&container.owners);
        let mut i = 0;
        while (i < len) {
            if (*vector::borrow(&container.owners, i) == addr) {
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
        external_id: String,
        name: String,
        description: String,
        ctx: &mut TxContext
    ) {
        let owner = sender(ctx);

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
            owner,
            name: container.name,
            description: container.description,
        });

        transfer::transfer(container, owner);
    }

    public entry fun add_owner(
        container: &mut Container,
        new_owner: address,
        ctx: &mut TxContext
    ) {
        assert_owner(container, ctx);
        if (!is_owner(container, new_owner)) {
            vector::push_back(&mut container.owners, new_owner);
        };
    }

    public entry fun remove_owner(
        container: &mut Container,
        owner_to_remove: address,
        ctx: &mut TxContext
    ) {
        assert_owner(container, ctx);
        let len = vector::length(&container.owners);
        let mut i = 0;
        while (i < len) {
            if (*vector::borrow(&container.owners, i) == owner_to_remove) {
                vector::swap_remove(&mut container.owners, i);
                return;
            };
            i = i + 1;
        };
    }

    public entry fun attach_container_child(
        parent: &mut Container,
        child: &Container,
        ctx: &mut TxContext
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
        external_id: String,
        name: String,
        description: String,
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
        external_id: String,
        name: String,
        content: String,
        ctx: &mut TxContext
    ) {
        assert_owner(container, ctx);
        assert!(data_type.container == object::id(container), 2);

        let creator = sender(ctx);

        let item = DataItem {
            id: object::new(ctx),
            container: object::id(container),
            data_type: object::id(data_type),
            creator,
            external_id,
            name,
            content,
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
            creator,
            name: item.name,
        });

        transfer::transfer(item, creator);
    }
}
