module sendit_messenger::generic_store {
    use std::vector;
    use std::string::String;

    use iota::object::{Self, UID, ID};
    use iota::tx_context::{TxContext, sender};
    use iota::transfer;
    use iota::event;
    use iota::clock::{Clock, timestamp_ms};
    use iota::address;

    /// ==========================
    /// SHARED LIGHTWEIGHT REF
    /// ==========================
    public struct ObjectRef has copy, drop, store {
        object_id: ID,
        external_id: String,
    }

    /// ==========================
    /// OWNER STRUCT
    /// ==========================
    public struct Owner has key, store {
          id: UID,
        addr: address,  // actual IOTA address
        role: String,   // e.g., "owner", "editor"
    }

    /// ==========================
    /// OBJECTS
    /// ==========================
    public struct Container has key, store {
        id: UID,
        owners: vector<Owner>,  // store Owner structs
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
        creator: String, // store as string
        external_id: String,
        name: String,
        content: String,
        day_tag: u16, // 1..365 or 366
    }

    /// ==========================
    /// EVENTS — CREATION
    /// ==========================
    public struct ContainerCreatedEvent has copy, drop {
        object_id: ID,
        external_id: String,
        owner: String, // now string
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
        creator: String,
        name: String,
        day_tag: u16,
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
            if (vector::borrow(&container.owners, i).addr == caller) {
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
            if (vector::borrow(&container.owners, i).addr == addr) {
                return true;
            };
            i = i + 1;
        };
        false
    }

    /// ==========================
    /// TIME HELPERS
    /// ==========================
    const MS_PER_DAY: u64 = 86_400_000;

    fun is_leap_year(year: u64): bool {
        (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
    }

    fun year_and_day_of_year(days_since_epoch: u64): (u64, u16) {
        let mut year = 1970u64;
        let mut days = days_since_epoch;

        loop {
            let year_len = if (is_leap_year(year)) { 366 } else { 365 };
            if (days < year_len) break;
            days = days - year_len;
            year = year + 1;
        };

        (year, (days + 1) as u16)
    }

    fun day_of_year(clock: &Clock): u16 {
        let ms = timestamp_ms(clock);
        let days_since_epoch = ms / MS_PER_DAY;
        let (_, day) = year_and_day_of_year(days_since_epoch);
        day
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
        let owner_addr = sender(ctx);

        let owner = Owner {
             id: object::new(ctx),
            addr: owner_addr,
            role: std::string::utf8(b"owner"),
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
            external_id,
            owner:  address::to_string(owner_addr),
            name,
            description,
        });

        transfer::transfer(container, owner_addr);
    }
 public entry fun add_owner(
        container: &mut Container,
        new_owner: address,
        role: String,
        ctx: &mut TxContext
    ) {
       /* let owner_addr = sender(ctx);
        let owner = Owner {
             id: object::new(ctx),
                        addr: new_owner,
                        role,
                    };
           */
        //assert_owner(container, ctx);
       // if (!is_owner(container, new_owner)) {
            vector::push_back(&mut container.owners, Owner {
             id: object::new(ctx),
                        addr: new_owner,
                        role,
                    });
       // };
        // transfer::share_object(owner);
    }

/*
    public entry fun remove_owner(
        container: &mut Container,
        owner_to_remove: address,
        ctx: &mut TxContext
    ) {
        assert_owner(container, ctx);
        let len = vector::length(&container.owners);
        let mut i = 0;
        while (i < len) {
            if (vector::borrow(&container.owners, i).addr == owner_to_remove) {
                vector::swap_remove(&mut container.owners, i);
                return;
            };
            i = i + 1;
        };
    }*/

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
            name: name,
            description: description,
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
        clock: &Clock,
        ctx: &mut TxContext
    ) {
        assert_owner(container, ctx);
        assert!(data_type.container == object::id(container), 2);

        let creator_str = address::to_string(sender(ctx));
        let day_tag = day_of_year(clock);

        let item = DataItem {
            id: object::new(ctx),
            container: object::id(container),
            data_type: object::id(data_type),
            creator: creator_str,
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
            creator: creator_str,
            name: item.name,
            day_tag,
        });

        transfer::transfer(item, sender(ctx));
    }
}
