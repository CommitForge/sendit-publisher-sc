module sendit_messenger::indexer {

    use std::vector;
    use std::option;
    use iota::object::{Self, UID, ID};
    use iota::tx_context::TxContext;
    use iota::transfer;

    /// Lightweight reference (reuse yours if you want)
    public struct ObjectRef has copy, drop, store {
        object_id: ID,
        external_id: vector<u8>, // bytes to avoid string deps
    }

    /// One page of an index
    public struct IndexPage has key, store {
        id: UID,
        parent: ID,          // owning object (container)
        kind: u8,            // 1=owners,2=children,3=types,4=items
        page: u32,
        items: vector<ObjectRef>,
        next: option::Option<ID>,
    }

    const MAX_PAGE_SIZE: u64 = 20;

    /// ==========================
    /// CREATE ROOT PAGE
    /// ==========================
    public fun new_root(
        parent: ID,
        kind: u8,
        ctx: &mut TxContext
    ) {
       transfer::transfer(obj, recipient)( IndexPage {
            id: object::new(ctx),
            parent,
            kind,
            page: 0,
            items: vector::empty(),
            next: option::none(),
        }
        );
    }

    /// ==========================
    /// APPEND ITEM (CORE LOGIC)
    /// ==========================
    public fun push(
        page: &mut IndexPage,
        obj: ObjectRef,
        ctx: &mut TxContext
    ) {
        let len = vector::length(&page.items);

        if (len < MAX_PAGE_SIZE) {
            vector::push_back(&mut page.items, obj);
            return;
        };

        // create next page
        let next = IndexPage {
            id: object::new(ctx),
            parent: page.parent,
            kind: page.kind,
            page: page.page + 1,
            items: vector::singleton(obj),
            next: option::none(),
        };

        let next_id = object::id(&next);
        page.next = option::some(next_id);

        transfer::share_object(next);
    }
}
