use std::ptr;
use std::time::Instant;
use std::hint::spin_loop;
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::collections::VecDeque;
use std::cmp::{Ord, PartialOrd, Eq, PartialEq, Ordering};
use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, AtomicPtr, Ordering as AtomicOrdering}};

use crate::vpm::PageId;

/// 默认的最小块系数
const DEFAULT_MIN_B: usize = 4;

/// 默认的最大块系数
const DEFAULT_MAX_B: usize = 65535;

///
/// 写时复制的无锁并发B+树映射表
///
#[derive(Debug, Clone)]
pub struct CowBtreeMap<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
>(Arc<InnerCowBtreeMap<K, V>>);

unsafe impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Send for CowBtreeMap<K, V> {}
unsafe impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Sync for CowBtreeMap<K, V> {}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> CowBtreeMap<K, V> {
    /// 构建一个指定块系数的空的写时复制的无锁并发B+树映射表
    pub fn empty(mut b: usize) -> Self {
        if b < DEFAULT_MIN_B {
            b = DEFAULT_MIN_B;
        } else if b > DEFAULT_MAX_B {
            b= DEFAULT_MAX_B;
        }

        let leaf = Arc::new(Leaf::<K, V>::empty(b));
        let node = Arc::new(Node::Leaf(leaf.clone()));
        let root = AtomicPtr::new(Arc::into_raw(node) as *mut Node<K, V>);
        let inner = InnerCowBtreeMap {
            b,
            length: AtomicU64::new(0),
            depth: AtomicUsize::new(0),
            root,
        };

        CowBtreeMap(Arc::new(inner))
    }

    /// 构建一个指定块系数和初始键值对的写时复制的无锁并发B+树映射表
    pub fn with_pairs(b: usize, pairs: Vec<(K, V)>) -> Self {
        let map = Self::empty(b);

        //插入初始键值对
        for (key, value) in pairs {
            map.upsert(key, value, None);
        }

        map
    }

    /// 对指定的原树进行复制，对复制的树进行的所有修改，都不会影响原树
    /// 只有在对复制的树进行手动提交后，原树才会被修改
    pub fn copy(source: &Self) -> Self {
        //复制块系数、键值对数据和树的深度
        let b = source.b_factor();
        let length = AtomicU64::new(source.len());
        let depth = AtomicUsize::new(source.depth());

        //深度复制树的根节点
        let (_raw, shared_root) = safety_borrow_root(&source.0.root, 3);
        let root_copy = shared_root.copy_on_write();
        let raw = Arc::into_raw(Arc::new(root_copy)) as *mut Node<K, V>;
        let root = AtomicPtr::new(raw);

        let inner = InnerCowBtreeMap {
            b,
            length,
            depth,
            root,
        };
        CowBtreeMap(Arc::new(inner))
    }

    /// 获取块系数
    pub fn b_factor(&self) -> usize {
        self.0.b
    }

    /// 获取映射表中键值对的数量
    pub fn len(&self) -> u64 {
        self.0.length.load(AtomicOrdering::Acquire)
    }

    /// 获取树的深度
    pub fn depth(&self) -> usize {
        self.0.depth.load(AtomicOrdering::Acquire)
    }

    /// 检查指定的关键字是否存在
    pub fn contains_key(&self, key: &K) -> bool {
        if let Some(_) = self.get(key) {
            true
        } else {
            false
        }
    }

    /// 获取最小关键字
    pub fn min_key<'a>(&self) -> Option<KeyRefGuard<'a, K, V>> {
        let (raw, shared_root) = safety_borrow_root(&self.0.root, 3);

        let root = unsafe {
            &*raw
        };
        let mut node = root;

        loop {
            match node {
                Node::Leaf(leaf) => {
                    //叶节点
                    if let Some(pair) = leaf.pairs.get(0) {
                        return Some(KeyRefGuard {
                            root: shared_root,
                            key: &pair.key,
                        });
                    } else {
                        return None;
                    }
                },
                Node::NonLeaf(non_leaf) => {
                    //非叶节点，则继续查找
                    let pair = non_leaf.pairs.get(0).unwrap();
                    node = pair.prev.as_ref().unwrap();
                },
            }
        }
    }

    /// 获取最大关键字
    pub fn max_key<'a>(&self) -> Option<KeyRefGuard<'a, K, V>> {
        let (raw, shared_root) = safety_borrow_root(&self.0.root, 3);

        let root = unsafe {
            &*raw
        };
        let mut node = root;

        loop {
            match node {
                Node::Leaf(leaf) => {
                    //叶节点
                    if let Some(pair) = leaf
                        .pairs
                        .get(leaf.len().checked_sub(1).unwrap_or(0)) {
                        return Some(KeyRefGuard {
                            root: shared_root,
                            key: &pair.key,
                        });
                    } else {
                        return None;
                    }
                },
                Node::NonLeaf(non_leaf) => {
                    //非叶节点，则继续查找
                    let pair = non_leaf
                        .pairs
                        .get(non_leaf.len() - 1)
                        .unwrap();
                    node = pair.next.as_ref().unwrap();
                },
            }
        }
    }

    /// 查询指定关键字的值的只读引用
    pub fn get<'a>(&self, key: &'a K) -> Option<ValueRefGuard<'a, K, V>> {
        let (raw, shared_root) = safety_borrow_root(&self.0.root, 3);

        let root = unsafe {
            &*raw
        };
        if let Ok(value) = query(root, key, false, &mut Vec::new()) {
            Some(ValueRefGuard {
                root: shared_root,
                value,
            })
        } else {
            None
        }
    }

    /// 获取指定关键字和顺序的关键字迭代器
    pub fn keys<'a>(&'a self,
                    key: Option<&'a K>,
                    descending: bool) -> KeyIterator<'a, K, V> {
        let (_raw, shared_root) = safety_borrow_root(&self.0.root, 3);
        if descending {
            KeyIterator::Descending(KeyDescendingIterator::new(self, shared_root, key))
        } else {
            KeyIterator::Ascending(KeyAscendingIterator::new(self, shared_root, key))
        }
    }

    /// 获取指定关键字和顺序的键值对迭代器
    pub fn values<'a>(&'a self,
                      key: Option<&'a K>,
                      descending: bool) -> KVPairIterator<'a, K, V> {
        let (_raw, shared_root) = safety_borrow_root(&self.0.root, 3);
        if descending {
            KVPairIterator::Descending(KVPairDescendingIterator::new(self, shared_root, key))
        } else {
            KVPairIterator::Ascending(KVPairAscendingIterator::new(self, shared_root, key))
        }
    }

    /// 插入或更新指定关键字的值，成功则返回指定关键字的旧值，超时则返回原关键字和值
    pub fn upsert(&self,
                  key: K,
                  value: V,
                  timeout: Option<u128>) -> Result<Option<V>, (K, V)> {
        let (raw, shared_root) = safety_borrow_root(&self.0.root, 3);

        let mut nodes = Vec::with_capacity(self.depth() + 1); //初始化查询栈
        let root = unsafe {
            &*raw
        };
        let result = query(root, &key, true, &mut nodes);
        match result {
            Err(index) => {
                //指定关键字的值不存在，则在指定位置插入
                let new =
                    insert(&mut nodes,
                           Some(index),
                           key.clone(),
                           value.clone()); //插入指定关键字的键值对

                if let None = safety_modify_tree(self, new, timeout) {
                    //安全的修改树超时，则立即返回原关键字和值
                    return Err((key, value));
                }
                drop(shared_root); //安全的修改树成功，则立即释放旧根节点的共享引用，保证旧根节点在共享引用计数清0后释放

                self.0.length.fetch_add(1, AtomicOrdering::Release);
                Ok(None)
            },
            Ok(_last_value_ref) => {
                //指定关键字的值存在，则更新
                let (new, last_value) = update(&mut nodes, &key, value.clone()); //更新指定关键字的值

                if let None = safety_modify_tree(self, new, timeout) {
                    //安全的修改树超时，则立即返回原关键字和值
                    return Err((key, value));
                }
                drop(shared_root); //安全的修改树成功，则立即释放旧根节点的共享引用，保证旧根节点在共享引用计数清0后释放

                Ok(Some(last_value))
            },
        }
    }

    /// 直接插入或更新指定关键字的值，成功则返回指定关键字的旧值，超时则返回原关键字和值
    /// 直接插入或更新不会写时复制任何节点
    pub fn direct_upsert(&self,
                         key: K,
                         value: V,
                         timeout: Option<u128>) -> Result<Option<V>, (K, V)> {
        //TODO 需要调用非写时复制的查询、非写时复制的删除和非写时复制的插入方法...
        Ok(None)
    }

    /// 移除指定关键字的值，成功则返回指定关键字的值，超时则返回原关键字的引用
    pub fn remove<'a>(&'a self,
                      key: &'a K,
                      timeout: Option<u128>) -> Result<Option<V>, &'a K> {
        let (raw, shared_root) = safety_borrow_root(&self.0.root, 3);

        let mut nodes = Vec::with_capacity(self.depth() + 1); //初始化查询栈
        let root = unsafe {
            &*raw
        };
        let result = query(root, &key, true, &mut nodes);
        match result {
            Err(_index) => {
                //指定关键字的值不存在，则返回
                drop(shared_root); //保证旧根节点在共享引用计数清0后释放
                Ok(None)
            },
            Ok(_last_value) => {
                //指定关键字的值存在，则更新
                let (new, last_value) = delete(&mut nodes, key).unwrap(); //删除指定关键字的键值对

                if let None = safety_modify_tree(self, new, timeout) {
                    //安全的修改树超时，则立即返回原关键字和值
                    return Err(key);
                }
                drop(shared_root); //安全的修改树成功，则立即释放旧根节点的共享引用，保证旧根节点在共享引用计数清0后释放

                self.0.length.fetch_sub(1, AtomicOrdering::Release);
                Ok(Some(last_value))
            },
        }
    }

    /// 直接移除指定关键字的值，成功则返回指定关键字的值，超时则返回原关键字的引用
    /// 直接移除不会写时复制任何节点
    pub fn direct_remove(&self,
                         key: K,
                         timeout: Option<u128>) -> Result<Option<V>, (K, V)> {
        //TODO 需要调用非写时复制的查询和非写时复制的删除...
        Ok(None)
    }

    /// 清空所有键值对
    pub fn clear(&self, timeout: Option<u128>) -> bool {
        let b = self.b_factor();
        let new = (b, Arc::new(Node::Leaf(Arc::new(Leaf::empty(b)))));

        if let None = safety_modify_tree(self, new, timeout) {
            //安全的修改树超时，则立即返回
            return false;
        }

        self.0.length.store(0, AtomicOrdering::Release);
        true
    }
}

// 安全的借用指定根节点的共享引用，并增加根节点共享引用的计数，返回根节点的指针和根节点的共享引用
// 因为借用了根节点的共享引用，所以可以保证在使用根节点的指针时，根节点不会被释放
// 注意根节点的共享引用被释放后，根节点的指针将不再安全
#[inline]
fn safety_borrow_root<K, V>(root: &AtomicPtr<Node<K, V>>, retry: u32) -> (*mut Node<K, V>, Arc<Node<K, V>>)
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    let mut raw = root.load(AtomicOrdering::Acquire);
    loop {
        if raw.is_null() {
            //替换失败，其它操作正在借用指定根节点的共享引用，则自旋后再次获取当前根节点，并重试
            spin(retry);
            raw = root.load(AtomicOrdering::Acquire);
            continue;
        }

        match root.compare_exchange_weak(raw,
                                         ptr::null_mut(),
                                         AtomicOrdering::AcqRel,
                                         AtomicOrdering::Acquire) {
            Err(new_raw) => {
                if new_raw.is_null() {
                    //替换失败，其它操作正在借用指定根节点的共享引用，则自旋后再次获取当前根节点，并重试
                    spin(retry);
                    raw = root.load(AtomicOrdering::Acquire);
                    continue;
                } else {
                    //替换失败，其它操作已经更新了根节点，则自旋后重试
                    raw = new_raw;
                    spin(retry);
                    continue;
                }
            },
            Ok(current_raw) => {
                //替换成功
                let shared_root = unsafe { Arc::from_raw(current_raw as *mut Node<K, V>) };
                let shared_root_copy = shared_root.clone(); //复制当前根节点的共享引用
                root.store(current_raw, AtomicOrdering::Release); //恢复被替换的当前根节点
                let _ = Arc::into_raw(shared_root); //防止被提前释放
                return (raw, shared_root_copy);
            },
        }
    }
}

// 安全的替换当前根节点的共享引用，成功返回上一个根节点的共享引用，超时返回空
// 上一个根节点会在所有借用了上一个根节点的共享引用都回收时释放
#[inline]
fn safety_replace_root<K, V>(current: &AtomicPtr<Node<K, V>>,
                             new: Arc<Node<K, V>>,
                             retry: u32,
                             timeout: Option<u128>) -> Option<Arc<Node<K, V>>>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    let mut current_raw = current.load(AtomicOrdering::Acquire);
    let mut new_raw = Arc::into_raw(new) as *mut Node<K, V>;

    if let Some(timeout) = timeout {
        //指定了替换重试的超时时长，则在替换失败时继续重试，直到达到超时时间
        let now = Instant::now();
        while now.elapsed().as_millis() > timeout {
            if current_raw.is_null() {
                //替换失败，其它操作正在借用当产胆根节点的共享引用，则自旋后再次获取当前根节点，并重试
                spin(retry);
                current_raw = current.load(AtomicOrdering::Acquire);
                continue;
            }

            match current.compare_exchange_weak(current_raw,
                                                new_raw,
                                                AtomicOrdering::AcqRel,
                                                AtomicOrdering::Acquire) {
                Err(new_current_raw) => {
                    if new_current_raw.is_null() {
                        //替换失败，其它操作正在借用指定根节点的共享引用，则自旋后再次获取当前根节点，并重试
                        spin(retry);
                        current_raw = current.load(AtomicOrdering::Acquire);
                        continue;
                    } else {
                        //替换失败，其它操作已经更新了根节点，则自旋后重试
                        current_raw = new_current_raw;
                        spin(retry);
                        continue;
                    }
                },
                Ok(old_raw) => {
                    //替换成功
                    let old_shared_root = unsafe { Arc::from_raw(old_raw as *mut Node<K, V>) };
                    return Some(old_shared_root);
                },
            }
        }

        //替换重试已超时，则立即返回
        None
    } else {
        //未指定替换重试的超时时长，则不重试
        if current_raw.is_null() {
            //替换失败，其它操作正在借用当产胆根节点的共享引用，则立即返回
            spin(retry);
            current_raw = current.load(AtomicOrdering::Acquire);
            return None;
        }

        match current.compare_exchange_weak(current_raw,
                                            new_raw,
                                            AtomicOrdering::AcqRel,
                                            AtomicOrdering::Acquire) {
            Err(_) => {
                //替换失败，其它操作已经更新了根节点，则立即返回
                None
            },
            Ok(old_raw) => {
                //替换成功
                let old_shared_root = unsafe { Arc::from_raw(old_raw as *mut Node<K, V>) };
                Some(old_shared_root)
            },
        }
    }
}

// 安全的更新写操作完成后的树，成功则返回旧的根节点，超时则返回空
fn safety_modify_tree<K, V>(tree: &CowBtreeMap<K, V>,
                            (new_depth, new_root): (usize, Arc<Node<K, V>>),
                            timeout: Option<u128>) -> Option<Arc<Node<K, V>>>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    tree.0.depth.store(new_depth, AtomicOrdering::Release); //更新树深度

    //更新树根节点
    safety_replace_root(&tree.0.root,
                        new_root,
                        3,
                        timeout)
}

// 从根节点开始查询指定关键字的值，指定关键字不存在则返回关键字应该在叶节点的键值对列表中的位置
#[inline]
fn query<'a: 'b, 'b, K, V>(mut node: &'a Node<K, V>,
                           key: &'a K,
                           writable: bool,
                           stack: &'b mut Vec<(Node<K, V>, usize)>) -> Result<&'a V, usize>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    let mut child_index = 0; //当前节点在上级非叶节点的键子对列表中的位置
    let mut is_next_child = true; //当前节点是否是上级非叶节点的后继子节点

    if writable {
        //写操作的查询
        loop {
            //复制当前节点，并关联复制的节点与上级非叶节点
            let node_copy = node.copy_on_write(); //为后续的写操作复制节点
            match stack.last_mut() {
                None => {
                    //当前节点是根节点，则忽略修改复制的上级非叶节点中的子节点
                    ()
                },
                Some((Node::NonLeaf(ref mut non_leaf), _)) => {
                    //当前节点有上级非叶节点，则修改复制的上级非叶节点中的子节点
                    let non_leaf_mut = NonLeaf::get_mut(non_leaf); //非安全的获取当前节点的上级非叶节点的可写引用
                    if is_next_child {
                        //当前节点是上级非叶节点的后继子节点，则修改上级非叶节点的键子对列表中指定位置的后继子节点
                        non_leaf_mut.pairs[child_index].next = Some(node_copy.clone());
                    } else {
                        //当前节点是上级非叶节点的前趋子节点，则修改上级非叶节点的键子对列表中指定位置的前趋子节点
                        non_leaf_mut.pairs[child_index].prev = Some(node_copy.clone());
                    }
                },
                Some((Node::Leaf(_), _)) => {
                    //当前节点的上级节点，一定是非叶节点
                    panic!("Qurey cow b plus tree failed, key: {:?}, reason: require non leaf", key);
                }
            }

            let r = if node.is_leaf() {
                //叶节点，则查询指定关键字的值
                stack.push((node_copy, 0)); //将当前叶节点的上级非叶节点复制和当前节点在上级非叶节点的键子对列表中的位置加入查询栈
                return query_leaf(node.as_leaf().unwrap(), key);
            } else {
                //非叶节点，则获取下一个子节点，并继续查询
                let r = query_non_leaf(node.as_non_leaf().unwrap(), key);
                node = r.0;
                (r.1, r.2)
            };
            child_index = r.0;
            is_next_child = r.1;
            stack.push((node_copy, child_index)); //将当前节点的上级非叶节点复制和当前节点在上级非叶节点的键子对列表中的位置加入查询栈
        }
    } else {
        //读操作的查询
        loop {
            let node_copy = node.clone(); //为后续的读操作复制节点的共享引用

            child_index = if node.is_leaf() {
                //叶节点，则查询指定关键字的值
                stack.push((node_copy, 0)); //将当前叶节点的上级非叶节点复制和当前节点在上级非叶节点的键子对列表中的位置加入查询栈
                return query_leaf(node.as_leaf().unwrap(), key);
            } else {
                //非叶节点，则获取下一个子节点，并继续查询
                let r = query_non_leaf(node.as_non_leaf().unwrap(), key);
                node = r.0;
                r.1
            };
            stack.push((node_copy, child_index)); //将当前节点的上级非叶节点复制和当前节点在上级非叶节点的键子对列表中的位置加入查询栈
        }
    }
}

// 查询指定关键字在叶节点中的值，指定关键字不存在则返回关键字应该在叶节点的键值对列表中的位置
#[inline]
fn query_leaf<'a, K, V>(node: &'a Arc<Leaf<K, V>>, key: &'a K) -> Result<&'a V, usize>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    let index = node.index(key);
    if index == 0 {
        if let Some(min_key) = node.min_key() {
            if key != min_key {
                //关键字比当前叶节点中的最小关键字还小，则返回关键字应该在叶节点的键值对列表中的位置
                return Err(index);
            }
        } else {
            //关键字比当前叶节点中的最小关键字还小，则返回关键字应该在叶节点的键值对列表中的位置
            return Err(index);
        }
    } else if index == node.len().checked_sub(1).unwrap_or(0) {
        if let Some(max_key) = node.max_key() {
            if key != max_key {
                //关键字比当前叶节点中的最大关键字还大，则返回关键字应该在叶节点的键值对列表中的位置
                return Err(index);
            }
        } else {
            //关键字比当前叶节点中的最大关键字还大，则返回关键字应该在叶节点的键值对列表中的位置
            return Err(index);
        }
    } else {
        if let Some(pair) = node.pairs.get(index) {
            if key != &pair.key {
                //关键字在当前叶节点中不存在，则返回关键字应该在叶节点的键值对列表中的位置
                return Err(index);
            }
        } else {
            //关键字在当前叶节点中不存在，则返回关键字应该在叶节点的键值对列表中的位置
            return Err(index);
        }
    }

    //关键字在当前叶节点中存在，则返回对应的值的只读引用
    Ok(&node.pairs[index].value)
}

// 查询非叶节点，并返回子节点、子节点在非叶节点的键子对列表中的位置和子节点是否是后继子节点
#[inline]
fn query_non_leaf<'a, K, V>(node: &'a Arc<NonLeaf<K, V>>, key: &'a K)
    -> (&'a Node<K, V>, usize, bool)
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    // 获取指定关键字对应的子节点序号
    let mut index = node.index(key);
    let last_index = node.len() - 1;
    if index == 0 {
        let pair = node.pairs.get(index).unwrap();
        if key < &pair.key {
            //关键字比当前非叶节点中的最小关键字还小，则返回最小关键字的前趋子节点
            (node
                 .pairs[index]
                 .prev
                 .as_ref()
                 .unwrap(), index, false)
        } else {
            //关键字大于等于当前非叶节点中的最小关键字，则返回最小关键字的后继子节点
            (node
                 .pairs[index]
                 .next
                 .as_ref()
                 .unwrap(), index, true)
        }
    } else if index == last_index {
        //关键字小于等于当前非叶节点中的最大关键字，但比当前非叶节点中的次大关键字大，则返回次大关键字的后继子节点
        let pair = node.pairs.get(index).unwrap();
        if key < &pair.key {
            //关键字比当前非叶节点中的最大关键字小，但比次大关键字大，则返回次小关键字的后继子节点
            (node
                 .pairs[index - 1]
                 .next
                 .as_ref()
                 .unwrap(), index - 1, true)
        } else {
            //关键字等于当前非叶节点中的最大关键字，则返回最大关键字的后继子节点
            (node
                 .pairs[index]
                 .next
                 .as_ref()
                 .unwrap(), index, true)
        }
    } else if index > last_index {
        //关键字比当前非叶节点中的最大关键字还大，则返回最大关键字的后继子节点
        (node
            .pairs[last_index]
            .next
            .as_ref()
            .unwrap(), last_index, true)
    } else {
        let pair = node.pairs.get(index).unwrap();
        if key < &pair.key {
            //关键字比当前非叶节点中的指定关键字小，则返回比指定关键字小的下一个关键字的后继子节点
            (node
                 .pairs[index - 1]
                 .next
                 .as_ref()
                 .unwrap(), index - 1, true)
        } else {
            //关键字大于等于前非叶节点中的指定关键字，则返回指定关键字的后继子节点
            (node
                 .pairs[index]
                 .next
                 .as_ref()
                 .unwrap(), index, true)
        }
    }
}

// 从叶节点开始插入键值对，返回插入后的树的深度和新根节点的共享引用
#[inline]
fn insert<K, V>(stack: &mut Vec<(Node<K, V>, usize)>,
                index: Option<usize>,
                key: K,
                value: V)
    -> (usize, Arc<Node<K, V>>)
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    if let Some((Node::Leaf(mut leaf), _)) = stack.pop() {
        //插入叶节点
        insert_leaf(&mut leaf, index, key, value);
        let leaf = try_split_leaf(stack, leaf);

        let mut depth = 0; //插入后的树深度
        if stack.len() > 0 {
            //根节点是非叶节点，则插入非叶节点
            let mut node = None;
            while let Some((Node::NonLeaf(mut non_leaf), _)) = stack.pop() {
                let non_leaf = try_split_non_leaf(stack, non_leaf);
                node = Some(Arc::new(Node::NonLeaf(non_leaf)));
                depth += 1; //记录插入后的树深度
            }

            (depth, node.unwrap())
        } else {
            //根节点是叶节点
            (depth, Arc::new(Node::Leaf(leaf)))
        }
    } else {
        //节点栈中有且至少有一个节点，且首次弹出的节点一定是叶节点
        panic!("Insert cow b plus tree failed, key: {:?}, value: {:?}, reason: require leaf",
               key,
               value);
    }
}

// 将键值对插入指定的叶节点
#[inline]
fn insert_leaf<K, V>(node: &mut Arc<Leaf<K, V>>,
                     index: Option<usize>,
                     key: K,
                     value: V)
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    let leaf_mut = Leaf::get_mut(node); //非安全的获取当前叶节点的可写引用
    if let Some(index) = index {
        //将指定键值对插入叶节点的键值对列表中的指定位置
        leaf_mut.insert(index, key, value);
    } else {
        //将键值对插入叶节点的键值对列表中的合适位置
        let index = leaf_mut.index(&key);
        leaf_mut.insert(index, key, value);
    }
}

// 尝试分裂指定的叶节点，返回分裂后的当前叶节点的共享引用
// 如果需要分裂，则修改待分裂的叶节点和叶节点的上级非叶节点，如果当前叶节点没有上级非叶节点，则创建当前叶节点的上级非叶节点
// 分裂后的叶节点，会互相指向对方
// 如果不需要分裂，则不会修改当前叶节点
#[inline]
fn try_split_leaf<K, V>(stack: &mut Vec<(Node<K, V>, usize)>,
                        mut leaf: Arc<Leaf<K, V>>) -> Arc<Leaf<K, V>>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    if !is_require_split_leaf(leaf.as_ref()) {
        //不需要分裂当前叶节点
        return leaf;
    }

    //分裂当前叶节点
    let leaf_mut = Leaf::get_mut(&leaf); //非安全的获取当前叶节点的可写引用
    let (split_index, split_key) = get_leaf_split_key(leaf_mut);
    let new_pairs = leaf_mut.pairs.split_off(split_index); //分裂当前叶节点的键值对列表
    let new_leaf = Arc::new(Leaf::new(leaf_mut.b, new_pairs));
    drop(leaf_mut); //对当前叶节点的修改已完成，则立即释放当前叶节点的可写引用

    if stack.len() == 0 {
        //需要分裂的当前叶节点是根节点，则创建上级非叶节点作为新的根节点
        let new_root =
            Arc::new(NonLeaf::new(leaf.b,
                                  split_key,
                                  Some(Node::Leaf(leaf.clone())),
                                  Some(Node::Leaf(new_leaf.clone()))));
        stack.push((Node::NonLeaf(new_root), 1)); //将新的根节点加入节点栈

        //分裂成功，则立即返回分裂后的当前叶节点
        return leaf;
    }

    //需要分裂的当前叶节点不是根节点，则需要修改当前叶节点的上级非叶节点
    if let (Node::NonLeaf(non_leaf), _) = stack.last_mut().unwrap() {
        let non_leaf_mut = NonLeaf::get_mut(non_leaf); //非安全的获取当前叶节点的上级非叶节点的可写引用
        let insert_index = non_leaf_mut.index(&split_key);

        //在当前叶节点的上级非叶节点的键子对列表中插入分裂关键字
        if insert_index > 0 {
            //分裂关键字大于上级非叶节点的最小关键字，则将新的叶节点作为分裂关键字的后继子节点
            non_leaf_mut.insert(insert_index,
                                split_key,
                                Some(Node::Leaf(new_leaf.clone())));
        } else {
            //分裂关键字大小于上级非叶节点的最小关键字，则首先将上级非叶节点的最小关键字的前趋子节点移除
            //然后将当前叶节点作为分裂关键字的前趋子节点，并将新的叶节点作为分裂关键字的后继子节点
            non_leaf_mut.pairs[0].prev = None;
            non_leaf_mut.push_front(split_key,
                                    Some(Node::Leaf(leaf.clone())),
                                    Some(Node::Leaf(new_leaf.clone())));
        }
    }

    //分裂成功
    leaf
}

// 判断指定叶节点是否需要分裂
#[inline]
fn is_require_split_leaf<K, V>(node: &Leaf<K, V>) -> bool
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    node.len() > node.b - 1
}

// 获取需要分裂的指定叶节点的分裂位置和分裂关键字
#[inline]
fn get_leaf_split_key<K, V>(node: &Leaf<K, V>) -> (usize, K)
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    let split_index = (node.len() as f64 / 2f64).floor() as usize;
    (split_index, node.pairs[split_index].key.clone())
}

// 尝试分裂指定的非叶节点，返回分裂后的当前非叶节点的共享引用
// 如果需要分裂，则修改待分裂的非叶节点和非节点的上级非叶节点，如果当前非叶节点没有上级非叶节点，则创建当前非叶节点的上级非叶节点
// 如果不需要分裂，则不会修改当前非叶节点
#[inline]
fn try_split_non_leaf<K, V>(stack: &mut Vec<(Node<K, V>, usize)>,
                            mut non_leaf: Arc<NonLeaf<K, V>>) -> Arc<NonLeaf<K, V>>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    if !is_require_split_non_leaf(non_leaf.as_ref()) {
        //不需要分裂当前非叶节点，则立即返回当前非叶节点
        return non_leaf;
    }

    //分裂当前非叶节点
    let non_leaf_mut = NonLeaf::get_mut(&non_leaf) ; //非安全的获取当前非叶节点的可写引用
    let (split_index, split_key) = get_non_leaf_split_key(non_leaf_mut);
    let mut new_pairs = non_leaf_mut.pairs.split_off(split_index); //分裂当前非叶节点的键子对列表
    let old_min_pair = new_pairs.pop_front().unwrap(); //从新的键子对的头部弹出旧的最小键子对
    new_pairs.get_mut(0).unwrap().prev = old_min_pair.next; //将旧的最小键子对的后继子节点作为新的键子对的新的最小键子对的前趋子节点
    let new_non_leaf = Arc::new(NonLeaf::with_pairs(non_leaf_mut.b, new_pairs)); //构建新的非叶节点
    drop(non_leaf_mut); //对当前非叶节点的修改已完成，则立即释放当前非叶节点的可写引用
    let split_key_prev = Some(Node::NonLeaf(non_leaf.clone())); //创建分裂关键字的前趋子节点
    let split_key_next = Some(Node::NonLeaf(new_non_leaf)); //创建分裂关键字的后继子节点

    if stack.len() == 0 {
        //需要分裂的当前非叶节点是根节点，则创建上级非叶节点作为新的根节点
        let new_root =
            Arc::new(NonLeaf::new(non_leaf.b, split_key, split_key_prev, split_key_next));
        stack.push((Node::NonLeaf(new_root), 1)); //将新的根节点加入节点栈

        //分裂成功，则立即返回分裂后的当前非叶节点
        non_leaf
    } else {
        //需要分裂的当前非叶节点不是根节点，则需要修改当前非叶节点的上级非叶节点
        if let (Node::NonLeaf(superior_non_leaf), _) = stack.last_mut().unwrap() {
            let superior_non_leaf_mut
                = NonLeaf::get_mut(superior_non_leaf); //非安全的获取当前非叶节点的上级非叶节点的可写引用
            let insert_index = superior_non_leaf_mut.index(&split_key);

            //在当前非叶节点的上级非叶节点的键子对列表中插入分裂关键字
            if insert_index > 0 {
                //分裂关键字大于上级非叶节点的最小关键字，则将新的非叶节点作为分裂关键字的后继子节点
                superior_non_leaf_mut.insert(insert_index,
                                             split_key,
                                             split_key_next);
            } else {
                //分裂关键字大小于上级非叶节点的最小关键字，则首先将上级非叶节点的最小关键字的前趋子节点移除
                //然后将当前非叶节点作为分裂关键字的前趋子节点，并将新的非叶节点作为分裂关键字的后继子节点
                superior_non_leaf_mut.pairs[0].prev = None;
                superior_non_leaf_mut.push_front(split_key,
                                                 split_key_prev,
                                                 split_key_next);
            }
        }

        //分裂成功，则立即返回分裂后的当前非叶节点
        non_leaf
    }
}

// 判断指定非叶节点是否需要分裂
#[inline]
fn is_require_split_non_leaf<K, V>(node: &NonLeaf<K, V>) -> bool
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    node.len() > node.b - 1
}

// 获取需要分裂的指定非叶节点的分裂位置和分裂关键字
#[inline]
fn get_non_leaf_split_key<K, V>(node: &NonLeaf<K, V>) -> (usize, K)
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    let split_index = (node.len() as f64 / 2f64).floor() as usize;
    (split_index, node.pairs[split_index].key.clone())
}

// 从叶节点开始更新键值对，返回新根节点的共享引用
#[inline]
fn update<K, V>(stack: &mut Vec<(Node<K, V>, usize)>,
                key: &K,
                value: V)
                -> ((usize, Arc<Node<K, V>>), V)
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    if let Some((Node::Leaf(mut leaf), _)) = stack.pop() {
        //更新叶节点
        let old_value = update_leaf(&mut leaf, key, value);

        let mut depth = 0; //插入后的树深度
        if stack.len() > 0 {
            //根节点是非叶节点
            let mut node = None;
            while let Some((Node::NonLeaf(non_leaf), _)) = stack.pop() {
                node = Some(Arc::new(Node::NonLeaf(non_leaf)));
                depth += 1; //记录插入后的树深度
            }

            ((depth, node.unwrap()), old_value)
        } else {
            //根节点是叶节点
            ((depth, Arc::new(Node::Leaf(leaf))), old_value)
        }
    } else {
        //节点栈中有且至少有一个节点，且首次弹出的节点一定是叶节点
        panic!("Update cow b plus tree failed, key: {:?}, value: {:?}, reason: require leaf",
               key,
               value);
    }
}

// 更新叶节点的指定关键字的值，并返回上一个值
#[inline]
fn update_leaf<K, V>(node: &mut Arc<Leaf<K, V>>,
                     key: &K,
                     value: V) -> V
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    let leaf_mut = Leaf::get_mut(node); //非安全的获取当前叶节点的可写引用

    let key_child_pair = leaf_mut.delete(key).unwrap();
    let old_value = key_child_pair.value;
    insert_leaf(node, None, key_child_pair.key, value);

    old_value
}

// 从叶节点开始删除键值对，返回删除后的树的深度和新根节点的共享引用
#[inline]
fn delete<'a, K, V>(nodes: &'a mut Vec<(Node<K, V>, usize)>, key: &K)
    -> Option<((usize, Arc<Node<K, V>>), V)>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    if let Some((Node::Leaf(mut leaf), _)) = nodes.pop() {
        //删除叶节点
        if let Some(key_value_pair) = delete_leaf(&mut leaf, key) {
            //指定关键字的键值对存在，并删除
            let leaf = try_merge_leaf(nodes, leaf, key);

            let mut depth = 0; //删除后的树深度
            if nodes.len() > 0 {
                //根节点是非叶节点，则删除非叶节点
                let mut node = None;
                while let Some((Node::NonLeaf(mut non_leaf), superior_child_index)) = nodes.pop() {
                    let non_leaf = try_merge_non_leaf(nodes, non_leaf, key);
                    node = Some(Arc::new(Node::NonLeaf(non_leaf)));
                    depth += 1; //记录删除后的树深度
                }

                Some(((depth, node.unwrap()), key_value_pair.value))
            } else {
                //根节点是叶节点
                Some(((depth, Arc::new(Node::Leaf(leaf))), key_value_pair.value))
            }
        } else {
            //指定关键字的键值对不存在
            None
        }
    } else {
        //节点栈中有且至少有一个节点，且首次弹出的节点一定是叶节点
        panic!("Delete cow b plus tree failed, key: {:?}, reason: require leaf", key);
    }
}

// 将键值对从指定的叶节点中删除
#[inline]
fn delete_leaf<K, V>(node: &mut Arc<Leaf<K, V>>, key: &K) -> Option<KeyValuePair<K, V>>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    let leaf_mut = Leaf::get_mut(node); //非安全的获取当前叶节点的可写引用

    //将键值对从叶节点的键值对列表中删除
    leaf_mut.delete(key)
}

// 尝试合并指定的叶节点，返回合并后的当前叶节点的共享引用
// 如果当前叶节点的键值对数量过少，则首先尝试从兄弟叶节点中借一个键值对
// 如果兄弟叶节点在借一个键值对后键值对数量也过少，则合并当前叶节点与兄弟叶节点
// 如果不需要合并，则不会修改当前叶节点
#[inline]
fn try_merge_leaf<K, V>(stack: &mut Vec<(Node<K, V>, usize)>,
                        mut leaf: Arc<Leaf<K, V>>,
                        key: &K) -> Arc<Leaf<K, V>>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    if !is_require_merge_leaf(leaf.as_ref(), false) {
        //不需要合并当前叶节点
        return leaf;
    }

    if stack.len() == 0 {
        //需要合并的当前叶节点是根节点，则忽略合并，并立即返回当前叶节点
        return leaf;
    }

    //需要合并的当前叶节点不是根节点
    if let (Node::NonLeaf(non_leaf), superior_child_index) = stack.pop().unwrap() {
        let (_, _, is_next_child) = query_non_leaf(&non_leaf, key);
        let max_non_leaf_index = non_leaf.len() - 1;
        if (max_non_leaf_index == 0 && superior_child_index == 0 && is_next_child)
            || (superior_child_index == max_non_leaf_index && is_next_child) {
            //当前叶节点是上级非叶节点的最大键子对的后继子节点，则从左兄弟叶节点借一个最大的键值对
            let leaf_left = if superior_child_index == 0 && is_next_child {
                non_leaf
                    .prev(superior_child_index.checked_sub(1).unwrap_or(0))
                    .unwrap()
                    .copy_on_write()
                    .into_leaf()
                    .unwrap() //因为需要借用左兄弟叶节点的键值对或将当前叶节点合并到左兄弟叶节点，所以需要复制左兄弟叶节点
            } else {
                non_leaf
                    .next(superior_child_index.checked_sub(1).unwrap_or(0))
                    .unwrap()
                    .copy_on_write()
                    .into_leaf()
                    .unwrap() //因为需要借用左兄弟叶节点的键值对或将当前叶节点合并到左兄弟叶节点，所以需要复制左兄弟叶节点
            };
            let leaf_left_mut = Leaf::get_mut(&leaf_left); //非安全的获取当前叶节点的左兄弟叶节点的可写引用

            if is_require_merge_leaf(leaf_left_mut, true) {
                //当前叶节点的左兄弟叶节点也需要合并
                let non_leaf_mut = NonLeaf::get_mut(&non_leaf); //非安全的获取当前叶节点的上级非叶节点的可写引用
                let leaf_mut = Leaf::get_mut(&leaf); //非安全的获取当前叶节点的可写引用

                let _key_child_pair = non_leaf_mut
                    .delete(superior_child_index)
                    .unwrap(); //因为当前叶节点合并到左兄弟非叶节点，则在上级非叶节点中删除当前叶节点对应的键子对
                leaf_left_mut.append(&mut leaf_mut.pairs); //将当前叶节点合并到左兄弟叶节点

                if stack.len() == 0 && non_leaf.len() == 0 {
                    //当前叶节点的上级非叶节点是根节点，则忽略对只有一个键子对的根节点的操作，也不用将根节点再加入节点栈
                    leaf_left
                } else {
                    //当前叶节点的左兄弟叶节点是普通叶节点
                    let new_superior_child_index = superior_child_index
                        .checked_sub(1)
                        .unwrap_or(0); //合并后的非叶节点在上级非叶节点中的新位置
                    non_leaf_mut
                        .replace_next(new_superior_child_index,
                                      Some(Node::Leaf(leaf_left.clone()))); //替换上级非叶节点指向的当前叶节点的左兄弟叶节点的复制

                    //将修改后的当前叶节点的上级非叶节点重新加入节点栈，并修改上级非叶点指向合并后的叶节点的位置
                    stack
                        .push((Node::NonLeaf(non_leaf.clone()),
                               new_superior_child_index));
                    leaf_left
                }
            } else {
                //当前叶节点的左兄弟叶节点可以借一个键值对
                let non_leaf_mut = NonLeaf::get_mut(&non_leaf); //非安全的获取当前叶节点的上级非叶节点的可写引用
                let leaf_mut = Leaf::get_mut(&leaf); //非安全的获取当前叶节点的可写引用

                let borrowed_key_value_pair = leaf_left_mut.pop_back().unwrap(); //移除当前叶节点的左兄弟叶节点的最大键子对
                let _replaced_key = non_leaf_mut
                    .replace_key(superior_child_index,
                                 borrowed_key_value_pair.key.clone()); //将当前叶节点的左兄弟叶节点的最大键子对的关键字作为当前非叶节点的上级非叶节点中被借用的关键字
                leaf_mut.push_front(borrowed_key_value_pair);
                if superior_child_index == 0 {
                    //替换上级非叶节点指向的当前叶节点的左兄弟叶节点的复制
                    non_leaf_mut
                        .replace_prev(superior_child_index,
                                      Some(Node::Leaf(leaf_left.clone())));
                } else {
                    //替换上级非叶节点指向的当前叶节点的左兄弟叶节点的复制
                    non_leaf_mut
                        .replace_next(superior_child_index.checked_sub(1).unwrap_or(0),
                                      Some(Node::Leaf(leaf_left.clone())));
                }

                //将修改后的当前叶节点的上级非叶节点重新加入节点栈
                stack.push((Node::NonLeaf(non_leaf), superior_child_index));
                leaf
            }
        } else {
            //当前叶节点是最小叶节点或普通叶节点，则从右兄弟叶节点借一个最小的键值对
            let (_, _, is_next_child) = query_non_leaf(&non_leaf, key);
            let leaf_right = if superior_child_index == 0 && !is_next_child {
                //当前叶节点上级非叶节点的最小子节点
                non_leaf
                    .next(superior_child_index)
                    .unwrap()
                    .as_leaf()
                    .unwrap()
            } else {
                //当前叶节点是普通叶节点
                non_leaf
                    .next(superior_child_index + 1)
                    .unwrap()
                    .as_leaf()
                    .unwrap()
            };

            if is_require_merge_leaf(leaf_right.as_ref(), true) {
                //当前叶节点的右兄弟叶节点也需要合并，则将右兄弟叶节点合并到当前叶节点
                let non_leaf_mut = NonLeaf::get_mut(&non_leaf); //非安全的获取当前叶节点的上级非叶节点的可写引用
                let leaf_mut = Leaf::get_mut(&leaf); //非安全的获取当前叶节点的可写引用
                let leaf_right_mut = Leaf::get_mut(leaf_right); //非安全的获取当前叶节点的右兄弟叶节点的可写引用

                if stack.len() == 0 && non_leaf.len() == 1 {
                    //当前叶节点的上级非叶节点是根节点，则忽略对只有一个键子对的根节点的操作，也不用将根节点再加入节点栈
                    leaf_mut.append(&mut leaf_right_mut.pairs); //将右兄弟叶节点合并到当前叶节点
                    leaf
                } else {
                    //当前叶节点是最小叶节点或普通叶节点，且当前叶节点的右兄弟叶节点是普通叶节点
                    if superior_child_index == 0 && !is_next_child {
                        //当前叶节点上级非叶节点的最小子节点
                        let key_child_pair = non_leaf_mut
                            .delete(superior_child_index)
                            .unwrap();
                        non_leaf_mut.pairs[0].prev = key_child_pair.prev; //将下移的键子对的前趋子节点作为当前叶节点的上级非叶节点的新的最小键子对的前趋子节点
                    } else {
                        //当前叶节点是普通叶节点
                        let _key_child_pair = non_leaf_mut
                            .delete(superior_child_index + 1)
                            .unwrap(); //因为当前叶节点合并到当前叶节点的右兄弟叶节点，则在上级非叶节点中删除当前叶节点的右兄弟叶节点对应的键子对
                    }
                    leaf_mut.append(&mut leaf_right_mut.pairs); //将右兄弟叶节点合并到当前叶节点

                    //将修改后的当前叶节点的上级非叶节点重新加入节点栈，不需要修改上级非叶点指向合并后的叶节点的位置
                    stack.push((Node::NonLeaf(non_leaf), superior_child_index));
                    leaf
                }
            } else {
                //当前叶节点的右兄弟叶节点可以借一个键值对
                let leaf_right_copy = leaf_right.copy_on_write(); //因为需要从右兄弟叶节点借用一个键值对，则需要复制右兄弟叶节点

                let non_leaf_mut = NonLeaf::get_mut(&non_leaf); //非安全的获取当前叶节点的上级非叶节点的可写引用
                let leaf_mut = Leaf::get_mut(&leaf); //非安全的获取当前叶节点的可写引用
                let leaf_right_mut = Leaf::get_mut(&leaf_right_copy); //非安全的获取当前叶节点的右兄弟叶节点的可写引用

                let superior_child_index = if superior_child_index == 0 && !is_next_child {
                    //当前叶节点上级非叶节点的最小子节点
                    superior_child_index
                } else {
                    //当前叶节点是普通叶节点
                    superior_child_index + 1
                };
                let borrowed_key_value_pair = leaf_right_mut.pop_front().unwrap(); //移除当前叶节点的右兄弟叶节点的最小键子对
                let _replaced_key = non_leaf_mut
                    .replace_key(superior_child_index,
                                 leaf_right_mut.pairs[0].key.clone()); //将当前叶节点的右兄弟叶节点的新的最小键子对的关键字作为当前叶节点的上级非叶节点中被借用的关键字
                leaf_mut.push_back(borrowed_key_value_pair);
                non_leaf_mut
                    .replace_next(superior_child_index,
                                  Some(Node::Leaf(leaf_right_copy))); //替换上级非叶节点指向的当前叶节点的右兄弟叶节点的复制

                //将修改后的当前叶节点的上级非叶节点重新加入节点栈
                stack.push((Node::NonLeaf(non_leaf), superior_child_index));
                leaf
            }
        }
    } else {
        //当前叶节点不是根节点，所以一定有上级非叶节点
        panic!("Merge leaf failed, reason: require non leaf");
    }
}

// 判断指定叶节点是否需要合并
#[inline]
fn is_require_merge_leaf<K, V>(node: &Leaf<K, V>, is_borrow: bool) -> bool
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    if is_borrow {
        node.len() <= (node.b as f64 / 2f64).floor() as usize
    } else {
        node.len() < (node.b as f64 / 2f64).floor() as usize
    }
}

// 尝试合并指定的非叶节点，返回合并后的当前非叶节点的共享引用
// 如果当前非叶节点的键值对数量过少，则首先尝试从兄弟非叶节点中借一个键值对
// 如果兄弟非叶节点在借一个键值对后键值对数量也过少，则合并当前非叶节点与兄弟非叶节点
// 如果不需要合并，则不会修改当前非叶节点
#[inline]
fn try_merge_non_leaf<K, V>(stack: &mut Vec<(Node<K, V>, usize)>,
                            mut non_leaf: Arc<NonLeaf<K, V>>,
                            key: &K) -> Arc<NonLeaf<K, V>>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    if !is_require_merge_non_leaf(non_leaf.as_ref(), false) {
        //不需要合并当前非叶节点
        return non_leaf;
    }

    if stack.len() == 0 {
        //需要合并的当前非叶节点是根节点，则忽略合并，并立即返回当前非叶节点
        return non_leaf;
    }

    //需要合并的当前非叶节点不是根节点
    if let (Node::NonLeaf(mut superior_non_leaf), superior_child_index) = stack.pop().unwrap() {
        let (_, _, is_next_child) = query_non_leaf(&superior_non_leaf, key);
        let max_superior_non_leaf_index = superior_non_leaf.len() - 1;
        if (max_superior_non_leaf_index == 0 && superior_child_index == 0 && is_next_child)
            || (superior_child_index == max_superior_non_leaf_index && is_next_child) {
            //当前非叶节点是上级非叶节点的最大键子对的后继子叶节点，则从左兄弟非叶节点借一个最大的键值对
            let non_leaf_left = if superior_child_index == 0 && is_next_child {
                if let Node::NonLeaf(non_leaf_left) = superior_non_leaf
                    .prev(superior_child_index.checked_sub(1).unwrap_or(0))
                    .unwrap()
                    .copy_on_write() {
                    //因为需要借用左兄弟非叶节点的键子对或将当前非叶节点合并到左兄弟非叶节点，则复制左兄弟非叶节点
                    non_leaf_left
                } else {
                    //当前非叶节点的左兄弟非叶节点，一定是非叶节
                    panic!("Merge non leaf failed, reason: require non leaf");
                }
            } else {
                if let Node::NonLeaf(non_leaf_left) = superior_non_leaf
                    .next(superior_child_index.checked_sub(1).unwrap_or(0))
                    .unwrap()
                    .copy_on_write() {
                    //因为需要借用左兄弟非叶节点的键子对或将当前非叶节点合并到左兄弟非叶节点，则复制左兄弟非叶节点
                    non_leaf_left
                } else {
                    //当前非叶节点的左兄弟非叶节点，一定是非叶节
                    panic!("Merge non leaf failed, reason: require non leaf");
                }
            };
            let non_leaf_left_mut = NonLeaf::get_mut(&non_leaf_left); //非安全的获取当前非叶节点的左兄弟非叶节点的可写引用

            if is_require_merge_non_leaf(non_leaf_left_mut, true) {
                //左兄弟非叶节点也需要合并
                let superior_non_leaf_mut = NonLeaf::get_mut(&superior_non_leaf); //非安全的获取当前非叶节点的上级非叶节点的可写引用
                let non_leaf_mut = NonLeaf::get_mut(&non_leaf); //非安全的获取当前非叶节点的可写引用

                let key_child_pair = superior_non_leaf_mut
                    .delete(superior_child_index)
                    .unwrap(); //因为当前非叶节点合并到左兄弟非叶节点，则在上级非叶节点中删除当前非叶节点对应的键子对
                non_leaf_left_mut.push_back(key_child_pair.key,
                                            None, //将下移的上级非叶节点键子对的前趋子节点设置为空
                                            non_leaf_mut.pairs[0].prev.take() //将当前非叶节点的最小键子对的前趋子节点作为下移的根节点键值子对的后继子节点
                ); //将修改后的下移的根节点键子对加入当前非叶节点的键子对队列尾
                non_leaf_left_mut.append(&mut non_leaf_mut.pairs); //将当前非叶节点合并到左兄弟非叶节点

                if stack.len() == 0 && superior_non_leaf.len() == 0 {
                    //当前非叶节点的上级非叶节点是根节点，且根节点只有唯一个键子对，则合并后生成新的根节点，旧的根节点不再加入节点栈
                    non_leaf_left
                } else {
                    //当前非叶节点的左兄弟非叶节点是普通非叶节点
                    let new_superior_child_index = superior_child_index
                        .checked_sub(1)
                        .unwrap_or(0); //合并后的非叶节点在上级非叶节点中的新位置
                    superior_non_leaf_mut
                        .replace_next(new_superior_child_index,
                                      Some(Node::NonLeaf(non_leaf_left.clone()))); //替换上级非叶节点指向的当前非叶节点的左兄弟非叶节点的复制

                    //将修改后的当前非叶节点的上级非叶节点重新加入节点栈，并修改上级非叶点指向合并后的非叶节点的位置
                    stack
                        .push((Node::NonLeaf(superior_non_leaf.clone()),
                               new_superior_child_index));

                    non_leaf_left
                }
            } else {
                //左兄弟非叶节点不需要合并，则从左兄弟非叶节点借一个最大的键子对
                let superior_non_leaf_mut = NonLeaf::get_mut(&superior_non_leaf); //非安全的获取当前非叶节点的上级非叶节点的可写引用
                let non_leaf_mut = NonLeaf::get_mut(&non_leaf); //非安全的获取当前非叶节点的可写引用

                let mut non_leaf_left_mut_min_key_child_pair = non_leaf_left_mut.pop_back().unwrap(); //移除当前非叶节点的左兄弟非叶节点的最大键子对
                let replaced_key = superior_non_leaf_mut
                    .replace_key(superior_child_index,
                                 non_leaf_left_mut_min_key_child_pair.key); //将当前非叶节点的左兄弟非叶节点的旧的最大键子对的关键字作为当前非叶节点为的上级非叶节点中被借用的关键字
                let new_prev = non_leaf_mut.pairs[0].prev.take(); //获取当前非叶节点的旧的最小键子对的前趋子节点
                non_leaf_mut.push_front(replaced_key, //将下移的键子对的关键字作为当前非叶节点的新的最键子对的关键字
                                        non_leaf_left_mut_min_key_child_pair.next.take(), //将当前非叶节点的左兄弟非叶节点的旧的最大键子对的后继子节点作为当前非叶节点的新的最小键值对的前趋子节点
                                        new_prev //将当前非叶节点的旧的最小键子对的前趋子节点作为当前非叶节点的新的最小键值对的后继子节点
                );

                if superior_child_index == 0 {
                    //替换上级非叶节点指向的当前叶节点的左兄弟叶节点的复制
                    superior_non_leaf_mut
                        .replace_prev(superior_child_index,
                                      Some(Node::NonLeaf(non_leaf_left.clone())));
                } else {
                    //替换上级非叶节点指向的当前叶节点的左兄弟叶节点的复制
                    superior_non_leaf_mut
                        .replace_next(superior_child_index.checked_sub(1).unwrap_or(0),
                                      Some(Node::NonLeaf(non_leaf_left.clone())));
                }

                //将修改后的当前非叶节点的上级非叶节点重新加入节点栈
                stack.push((Node::NonLeaf(superior_non_leaf), superior_child_index));
                non_leaf
            }
        } else {
            //当前非叶节点是兄弟节点中最小非叶节点或普通非叶节点，则从右兄弟非叶节点借一个最小的键子对
            let (_, _, is_next_child) = query_non_leaf(&superior_non_leaf, key);
            let non_leaf_right = if superior_child_index == 0 && !is_next_child {
                //当前非叶节点是上级非叶节点的最小子节点
                superior_non_leaf
                    .next(superior_child_index)
                    .unwrap()
                    .as_non_leaf()
                    .unwrap()
            } else {
                //当前非叶节点是普通非叶节点
                superior_non_leaf
                    .next(superior_child_index + 1)
                    .unwrap()
                    .as_non_leaf()
                    .unwrap()
            };

            if is_require_merge_non_leaf(non_leaf_right.as_ref(), true) {
                //当前非叶节点的右兄弟非叶节点也需要合并，则将右兄弟非叶节点合并到当前非叶节点
                let superior_non_leaf_mut = NonLeaf::get_mut(&superior_non_leaf); //非安全的获取当前非叶节点的上级非叶节点的可写引用
                let non_leaf_mut = NonLeaf::get_mut(&non_leaf); //非安全的获取当前非叶节点的可写引用
                let non_leaf_right_mut = NonLeaf::get_mut(non_leaf_right); //非安全的获取当前非叶节点的右兄弟非叶节点的可写引

                if stack.len() == 0 && superior_non_leaf.len() == 1 {
                    //当前非叶节点是兄弟节点中最小非叶节点，当前非叶节点的上级非叶节点是根节点，且根节点只有唯一个键子对，则合并后生成新的根节点，旧的根节点不再加入节点栈
                    let key_child_pair = superior_non_leaf_mut
                        .delete(superior_child_index)
                        .unwrap();
                    non_leaf_mut.push_back(key_child_pair.key,
                                           None, //将下移的上级非叶节点键子对的前趋子节点设置为空
                                           non_leaf_right_mut.pairs[0].prev.take() //将右兄弟非叶节点的最小键子对的前趋子节点作为下移的根节点键值子对的后继子节点
                    ); //将修改后的下移的根节点键子对加入当前非叶节点的键子对队列尾
                    non_leaf_mut.append(&mut non_leaf_right_mut.pairs); //将右兄弟非叶节点合并到当前非叶节点

                    non_leaf
                } else {
                    //当前非叶节点是兄弟节点中最小非叶节点或普通非叶节点，且当前非叶节点的右兄弟非叶节点是普通叶节点
                    if superior_child_index == 0 && !is_next_child {
                        //当前非叶节点是上级非叶节点的最小子节点
                        let key_child_pair = superior_non_leaf_mut
                            .delete(superior_child_index)
                            .unwrap();
                        superior_non_leaf_mut.pairs[0].prev = key_child_pair.prev; //将下移的键子对的前趋子节点作为当前非叶节点的上级非叶节点的新的最小键子对的前趋子节点
                        non_leaf_mut.push_back(key_child_pair.key,
                                               None, //将下移的上级非叶节点键子对的前趋子节点设置为空
                                               non_leaf_right_mut.pairs[0].prev.take() //将右兄弟非叶节点的最小键子对的前趋子节点作为下移的根节点键值子对的后继子节点
                        ); //将修改后的下移的根节点键子对加入当前非叶节点的键子对队列尾
                    } else {
                        //当前非叶节点是普通非叶节点
                        let key_child_pair = superior_non_leaf_mut
                            .delete(superior_child_index + 1)
                            .unwrap(); //因为右兄弟非叶节点合并到当前非叶节点，则在上级非叶节点中删除右兄弟非叶节点对应的键子对
                        non_leaf_mut.push_back(key_child_pair.key,
                                               None, //将下移的上级非叶节点键子对的前趋子节点设置为空
                                               non_leaf_right_mut.pairs[0].prev.take() //将右兄弟非叶节点的最小键子对的前趋子节点作为下移的根节点键值子对的后继子节点
                        ); //将修改后的下移的根节点键子对加入当前非叶节点的键子对队列尾
                    };
                    non_leaf_mut.append(&mut non_leaf_right_mut.pairs); //将右兄弟非叶节点合并到当前非叶节点

                    //将修改后的当前非叶节点的上级非叶节点重新加入节点栈，不需要修改上级非叶点指向合并后的非叶节点的位置
                    stack.push((Node::NonLeaf(superior_non_leaf), superior_child_index));
                    non_leaf
                }
            } else {
                //当前非叶节点的右兄弟非叶节点可以借一个键子对
                let non_leaf_right_copy = non_leaf_right.copy_on_write(); //因为需要从右兄弟非叶节点借用一个键子对，则需要复制右兄弟非叶节点

                let superior_non_leaf_mut = NonLeaf::get_mut(&superior_non_leaf); //非安全的获取当前非叶节点的上级非叶节点的可写引用
                let non_leaf_mut = NonLeaf::get_mut(&non_leaf); //非安全的获取当前非叶节点的可写引用
                let non_leaf_right_mut = NonLeaf::get_mut(&non_leaf_right_copy); //非安全的获取当前非叶节点的右兄弟非叶节点的可写引用

                let mut non_leaf_right_min_key_child_pair = non_leaf_right_mut.pop_front().unwrap(); //移除当前非叶节点的右兄弟非叶节点的最小键子对
                non_leaf_right_mut.pairs[0].prev = non_leaf_right_min_key_child_pair.next.take(); //将当前非叶节点的右兄弟非叶节点的旧的最小键子对的后继子节点作为右兄弟非叶节点的新的最小键值对的前趋子节点

                let superior_child_index = if superior_child_index == 0 && !is_next_child {
                    //当前非叶节点是上级非叶节点的最小子节点
                    superior_child_index
                } else {
                    //当前叶节点是普通叶节点
                    superior_child_index + 1
                };
                let replaced_key = superior_non_leaf_mut
                    .replace_key(superior_child_index,
                                 non_leaf_right_min_key_child_pair.key); //将当前非叶节点的右兄弟非叶节点的旧的最小键子对的关键字作为当前非叶节点为的上级非叶节点中被借用的关键字
                non_leaf_mut.push_back(replaced_key, //将下移的键子对的关键字作为当前非叶节点的新的最大键子对的关键字
                                       None,
                                       non_leaf_right_min_key_child_pair.prev.take() //将当前非叶节点的右兄弟非叶节点的旧的最小键子对的前趋子节点作为当前非叶节点的最大键子对的后继子节点
                );
                superior_non_leaf_mut
                    .replace_next(superior_child_index,
                                  Some(Node::NonLeaf(non_leaf_right_copy))); //替换上级非叶节点指向的当前非叶节点的右兄弟非叶节点的复制

                //将修改后的当前非叶节点的上级非叶节点重新加入节点栈
                stack.push((Node::NonLeaf(superior_non_leaf), superior_child_index));
                non_leaf
            }
        }
    } else {
        //当前非叶节点不是根节点，所以一定有上级非叶节点
        panic!("Merge non leaf failed, reason: require non leaf");
    }
}

// 判断指定非叶节点是否需要合并
#[inline]
fn is_require_merge_non_leaf<K, V>(node: &NonLeaf<K, V>, is_borrow: bool) -> bool
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    if is_borrow {
        node.len() <= (node.b as f64 / 2f64).floor() as usize
    } else {
        node.len() < (node.b as f64 / 2f64).floor() as usize
    }
}

// 内部写时复制的无锁并发B+树映射表
struct InnerCowBtreeMap<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    b:      usize,                  //块系数
    length: AtomicU64,              //键值对数量
    depth:  AtomicUsize,            //树深度
    root:   AtomicPtr<Node<K, V>>,  //根节点
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Debug for InnerCowBtreeMap<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let raw = self.root.load(AtomicOrdering::Acquire);
        if raw.is_null() {
            //当前根节点正在被使用
            return write!(f,
                          "b: {}, len: {}, depth: {}\n   {:#?}\n",
                          self.b,
                          self.length.load(AtomicOrdering::Acquire),
                          self.depth.load(AtomicOrdering::Acquire),
                          "...");
        }

        let shared_root = unsafe { Arc::from_raw(raw as *const Node<K, V>) };
        let result = write!(f,
                            "b: {}, len: {}, depth: {}\n   {:#?}\n",
                            self.b,
                            self.length.load(AtomicOrdering::Acquire),
                            self.depth.load(AtomicOrdering::Acquire),
                            shared_root);
        let _ = Arc::into_raw(shared_root); //避免提前释放

        result
    }
}

// 节点
#[derive(Clone)]
enum Node<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    Leaf(Arc<Leaf<K, V>>),          //叶节点
    NonLeaf(Arc<NonLeaf<K, V>>),    //非叶节点
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Debug for Node<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Node::Leaf(leaf) => {
                let pairs: Vec<&KeyValuePair<K, V>> = leaf.pairs.iter().collect();
                let pairs_len = pairs.len();
                if pairs_len > 5 {
                    write!(f, "#Leaf({})[{:#?}, ..., {:#?}]", pairs_len, &pairs[0], &pairs[pairs_len -1])
                } else {
                    write!(f, "#Leaf({}){:#?}", pairs_len, pairs.as_slice())
                }
            },
            Node::NonLeaf(non_leaf) => {
                let pairs: Vec<&KeyChildPair<K, V>> = non_leaf.pairs.iter().collect();
                let pairs_len = pairs.len();
                if pairs_len > 5 {
                    write!(f, "#NonLeaf({})[{:#?}, ..., {:#?}]", pairs_len, &pairs[0], &pairs[pairs_len -1])
                } else {
                    write!(f, "#NonLeaf({}){:#?}", pairs_len, pairs.as_slice())
                }
            },
        }
    }
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Node<K, V> {
    // 是否是叶节点
    #[inline]
    fn is_leaf(&self) -> bool {
        if let Node::Leaf(_) = self {
            true
        } else {
            false
        }
    }

    // 是否是非叶节点
    #[inline]
    fn is_non_leaf(&self) -> bool {
        if let Node::NonLeaf(_) = self {
            true
        } else {
            false
        }
    }

    // 写时复制，用于深度复制节点
    #[inline]
    fn copy_on_write(&self) -> Self {
        match self {
            Node::Leaf(leaf) => {
                //复制叶节点
                Node::Leaf(leaf.copy_on_write())
            },
            Node::NonLeaf(non_leaf) => {
                //复制非叶节点
                Node::NonLeaf(non_leaf.copy_on_write())
            },
        }
    }

    // 获取叶节点的只读引用
    #[inline]
    fn as_leaf(&self) -> Option<&Arc<Leaf<K, V>>> {
        if let Node::Leaf(leaf) = self {
            Some(leaf)
        } else {
            None
        }
    }

    // 获取非叶节点的只读引用
    #[inline]
    fn as_non_leaf(&self) -> Option<&Arc<NonLeaf<K, V>>> {
        if let Node::NonLeaf(non_leaf) = self {
            Some(non_leaf)
        } else {
            None
        }
    }

    // 转换为叶节点
    #[inline]
    fn into_leaf(self) -> Option<Arc<Leaf<K, V>>> {
        if let Node::Leaf(leaf) = self {
            Some(leaf)
        } else {
            None
        }
    }

    // 转换为非叶节点
    #[inline]
    fn into_non_leaf(self) -> Option<Arc<NonLeaf<K, V>>> {
        if let Node::NonLeaf(non_leaf) = self {
            Some(non_leaf)
        } else {
            None
        }
    }
}

// 叶节点
#[derive(Debug, Clone)]
struct Leaf<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    b:      usize,                          //块系数
    pairs:  VecDeque<KeyValuePair<K, V>>,   //键值对列表
    page:   Option<PageId>,                 //叶节点的逻辑页，非空表示叶节点需要持久化
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Leaf<K, V> {
    // 创建指定块系数的空叶节点
    #[inline]
    fn empty(b: usize) -> Self {
        Leaf {
            b,
            pairs: VecDeque::with_capacity(b - 1),
            page: None,
        }
    }

    // 创建指定块系数、键值对列表、左兄弟叶节点和右兄弟叶节点的叶节点
    fn new(b: usize, pairs: VecDeque<KeyValuePair<K, V>>) -> Self {
        Leaf {
            b,
            pairs,
            page: None,
        }
    }

    // 从指定叶节点的共享引用中获取可写引用
    #[inline]
    fn get_mut(leaf: &Arc<Leaf<K, V>>) -> &mut Leaf<K, V> {
        //通过指定叶节点的共享引用的指针，非安全的获取指定叶节点的可写指针
        let leaf_raw = Arc::as_ptr(&leaf) as *mut Leaf<K, V>;

        //非安全的获取指定叶节点的可写引用
        unsafe {
            &mut *leaf_raw
        }
    }

    // 获取键值对数量
    #[inline]
    fn len(&self) -> usize {
        self.pairs.len()
    }

    // 获取最小关键字
    #[inline]
    fn min_key<'a>(&'a self) -> Option<&'a K> {
        if let Some(pair) = self.pairs.get(0) {
            Some(&pair.key)
        } else {
            None
        }
    }

    // 获取最大关键字
    #[inline]
    fn max_key<'a>(&'a self) -> Option<&'a K> {
        if let Some(pair) = self
            .pairs
            .get(self.len().checked_sub(1).unwrap_or(0)) {
            Some(&pair.key)
        } else {
            None
        }
    }

    // 获取指定关键字在叶节点的键值对列表中的位置
    #[inline]
    fn index(&self, key: &K) -> usize {
        self
            .pairs
            .binary_search_by(|key_value_pair| {
                key_value_pair.key.cmp(key)
            })
            .unwrap_or_else(|index| index)
    }

    // 写时复制当前叶节点
    #[inline]
    fn copy_on_write(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }

    // 插入指定键值对到叶节点的键值对列表中的指定位置
    #[inline]
    fn insert(&mut self,
              index: usize,
              key: K,
              value: V) {
        self
            .pairs
            .insert(index, KeyValuePair::new(key, value));
    }

    // 加入指定键值对到叶节点的键值对列表头
    #[inline]
    fn push_front(&mut self, pair: KeyValuePair<K, V>) {
        self.pairs.push_front(pair);
    }

    // 加入指定键值对到叶节点的键值对列表尾
    #[inline]
    fn push_back(&mut self, pair: KeyValuePair<K, V>) {
        self.pairs.push_back(pair);
    }

    // 将指定键值对连接到指定叶节点的键值对的尾部
    #[inline]
    fn append(&mut self, pairs: &mut VecDeque<KeyValuePair<K, V>>) {
        self.pairs.append(pairs);
    }

    // 从叶节点的键值对列表中删除指定键值对，并返回被删除的键值对
    #[inline]
    fn delete(&mut self, key: &K) -> Option<KeyValuePair<K, V>> {
        if let Ok(index) = self
            .pairs
            .binary_search_by(|key_value_pair| {
                key_value_pair.key.cmp(key)
            }) {
            //指定关键字存在，则移除对应的键值对
            return self.pairs.remove(index);
        }

        None
    }

    // 从叶节点的键值对列表头弹出一个键值对
    #[inline]
    fn pop_front(&mut self) -> Option<KeyValuePair<K, V>> {
        self.pairs.pop_front()
    }

    // 从叶节点的键值对列表尾弹出一个键值对
    #[inline]
    fn pop_back(&mut self) -> Option<KeyValuePair<K, V>> {
        self.pairs.pop_back()
    }
}

// 键值对
#[derive(Clone)]
struct KeyValuePair<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    key:    K,  //关键字
    value:  V,  //值
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Ord for KeyValuePair<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> PartialOrd for KeyValuePair<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Eq for KeyValuePair<K, V> {}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> PartialEq for KeyValuePair<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Debug for KeyValuePair<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({:?}, {:?})", self.key, self.value)
    }
}


impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> KeyValuePair<K, V> {
    // 构建一个键值对
    #[inline]
    fn new(key: K, value: V) -> Self {
        KeyValuePair {
            key,
            value,
        }
    }
}

// 非叶节点的逻辑子节点页，用于需要持久化的当前非叶节点映射的前趋后继子节点页
#[derive(Debug, Clone)]
struct LogicChildPage {
    prev:   PageId,    //前趋逻辑页唯一id
    next:   PageId,    //后继逻辑页唯一id
}

// 非叶节点
#[derive(Debug, Clone)]
struct NonLeaf<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    b:      usize,                          //块系数
    pairs:  VecDeque<KeyChildPair<K, V>>,   //键子对列表
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> NonLeaf<K, V> {
    // 创建指定块系数、关键字、前趋子节点和后继子节点的非叶节点
    #[inline]
    fn new(b: usize,
           key: K,
           prev: Option<Node<K, V>>,
           next: Option<Node<K, V>>) -> Self {
        let mut pairs = VecDeque::new();
        pairs.push_back(KeyChildPair::new(key, prev, next));

        NonLeaf {
            b,
            pairs
        }
    }

    // 创建指定块系数、键子对的非叶节点
    #[inline]
    fn with_pairs(b: usize, pairs: VecDeque<KeyChildPair<K, V>>) -> Self {
        NonLeaf {
            b,
            pairs
        }
    }

    // 从指定叶节点的共享引用中获取可写引用
    #[inline]
    fn get_mut(non_leaf: &Arc<NonLeaf<K, V>>) -> &mut NonLeaf<K, V> {
        //通过指定非叶节点的共享引用的指针，非安全的获取指定非叶节点的可写指针
        let non_leaf_raw = Arc::as_ptr(&non_leaf) as *mut NonLeaf<K, V>;

        //非安全的获取指定非叶节点的可写引用
        unsafe {
            &mut *non_leaf_raw
        }
    }

    // 获取键子对数量
    #[inline]
    fn len(&self) -> usize {
        self.pairs.len()
    }

    // 获取最小关键字
    #[inline]
    fn min_key<'a>(&'a self) -> Option<&'a K> {
        if let Some(pair) = self.pairs.get(0) {
            Some(&pair.key)
        } else {
            None
        }
    }

    // 获取最大关键字
    #[inline]
    fn max_key<'a>(&'a self) -> Option<&'a K> {
        if let Some(pair) = self
            .pairs
            .get(self.len().checked_sub(1).unwrap_or(0)) {
            Some(&pair.key)
        } else {
            None
        }
    }

    // 获取指定关键字在非叶节点的键子对列表中的位置
    #[inline]
    fn index(&self, key: &K) -> usize {
        self
            .pairs
            .binary_search_by(|key_child_pair| {
                key_child_pair.key.cmp(key)
            })
            .unwrap_or_else(|index| index)
    }

    // 获取当前非叶节点的键子对列表中指定位置的键值对的前趋子节点
    #[inline]
    fn prev(&self, index: usize) -> Option<&Node<K, V>> {
        self.pairs[index].prev.as_ref()
    }

    // 获取当前非叶节点的键子对列表中指定位置的键值对的后继子节点
    #[inline]
    fn next(&self, index: usize) -> Option<&Node<K, V>> {
        self.pairs[index].next.as_ref()
    }

    // 写时复制当前非叶节点
    #[inline]
    fn copy_on_write(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }

    // 插入指定键值对到非叶节点的键子对列表中的指定位置
    #[inline]
    fn insert(&mut self,
              index: usize,
              key: K,
              child: Option<Node<K, V>>) {
        self
            .pairs
            .insert(index, KeyChildPair::new(key, None, child));
    }

    // 加入指定键子对到非叶节点的键子对列表头
    #[inline]
    fn push_front(&mut self,
                  key: K,
                  prev: Option<Node<K, V>>,
                  next: Option<Node<K, V>>) {
        self
            .pairs
            .push_front(KeyChildPair::new(key, prev, next));
    }

    // 加入指定键子对到非叶节点的键子对列表尾
    #[inline]
    fn push_back(&mut self,
                  key: K,
                  prev: Option<Node<K, V>>,
                  next: Option<Node<K, V>>) {
        self
            .pairs
            .push_back(KeyChildPair::new(key, prev, next));
    }

    // 将指定键子对连接到指定非叶节点的键子对的尾部
    #[inline]
    fn append(&mut self, pairs: &mut VecDeque<KeyChildPair<K, V>>) {
        self.pairs.append(pairs);
    }

    // 从非叶节点的键值对列表中删除指定位置的键子对，并返回被删除的键子对
    #[inline]
    fn delete(&mut self, index: usize) -> Option<KeyChildPair<K, V>> {
        self.pairs.remove(index)
    }

    // 从非叶节点的键值对列表头弹出一个键子对
    #[inline]
    fn pop_front(&mut self) -> Option<KeyChildPair<K, V>> {
        self.pairs.pop_front()
    }

    // 从非叶节点的键值对列表尾弹出一个键子对
    #[inline]
    fn pop_back(&mut self) -> Option<KeyChildPair<K, V>> {
        self.pairs.pop_back()
    }

    // 替换非叶节点的键值对列表中指定位置的关键字，并返回被替换的关键字
    #[inline]
    fn replace_key(&mut self,
                   index: usize,
                   key: K) -> K {
        let key_child_pair = &mut self.pairs[index];
        let replaced_key = key_child_pair.key.clone();
        key_child_pair.key = key;

        replaced_key
    }

    // 替换非叶节点的键值对列表中指定位置的前趋子节点，并返回被替换的前趋子节点
    #[inline]
    fn replace_prev(&mut self,
                    index: usize,
                    prev: Option<Node<K, V>>) -> Option<Node<K, V>> {
        let key_child_pair = &mut self.pairs[index];
        let replaced_prev = key_child_pair.prev.take();
        key_child_pair.prev = prev;

        replaced_prev
    }

    // 替换非叶节点的键值对列表中指定位置的后继子节点，并返回被替换的后继子节点
    #[inline]
    fn replace_next(&mut self,
                    index: usize,
                    next: Option<Node<K, V>>) -> Option<Node<K, V>> {
        let key_child_pair = &mut self.pairs[index];
        let replaced_next = key_child_pair.next.take();
        key_child_pair.next = next;

        replaced_next
    }
}

// 键子对
#[derive(Clone)]
struct KeyChildPair<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    key:    K,                      //关键字
    prev:   Option<Node<K, V>>,     //前趋子节点，只有非叶节点中的最小关键字有前趋子节点
    next:   Option<Node<K, V>>,     //后继子节点
    page:   Option<PageId>,         //非叶节点的逻辑页，非空表示非叶节点需要持久化
    childs: Option<LogicChildPage>, //非叶节点的逻辑子节点叶
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Ord for KeyChildPair<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> PartialOrd for KeyChildPair<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Eq for KeyChildPair<K, V> {}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> PartialEq for KeyChildPair<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Debug for KeyChildPair<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({:?}, <{:#?}, {:#?}>)", self.key, self.prev, self.next)
    }
}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> KeyChildPair<K, V> {
    // 创建一个键子对
    #[inline]
    fn new(key: K,
           prev: Option<Node<K, V>>,
           next: Option<Node<K, V>>) -> Self {
        KeyChildPair {
            key,
            prev,
            next,
            page: None,
            childs: None,
        }
    }
}

///
/// 关键字的只读引用守护者，保证关键字的只读引用在使用时，被引用的关键字不会被释放
/// 当关键字的只读引用回收时，自动减少对根节点的引用计数
///
pub struct KeyRefGuard<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    root:   Arc<Node<K, V>>,    //根节点的共享引用
    key:  &'a K,                //关键字的只读引用
}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> KeyRefGuard<'a, K, V> {
    /// 获取关键字的只读引用
    pub fn key(&'a self) -> &'a K {
        self.key
    }
}

///
/// 值的只读引用守护者，保证值的只读引用在使用时，被引用的值不会被释放
/// 当值的只读引用回收时，自动减少对根节点的引用计数
///
pub struct ValueRefGuard<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    root:   Arc<Node<K, V>>,    //根节点的共享引用
    value:  &'a V,              //值的只读引用
}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> ValueRefGuard<'a, K, V> {
    /// 获取值的只读引用
    pub fn value(&'a self) -> &'a V {
        self.value
    }
}

///
/// 关键字迭代器，保证关键字迭代器在使用时，被引用的关键字不会被释放
/// 当关键字迭代器回收时，自动减少对根节点的引用计数
///
pub enum KeyIterator<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    Ascending(KeyAscendingIterator<'a, K, V>),      //升序迭代器
    Descending(KeyDescendingIterator<'a, K, V>),    //降序迭代器
}

unsafe impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Send for KeyIterator<'a, K, V> {}
unsafe impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Sync for KeyIterator<'a, K, V> {}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> KeyIterator<'a, K, V> {
    /// 将关键字迭代器转换为关键字顺序迭代器
    pub fn into_ascending(self) -> KeyAscendingIterator<'a, K, V> {
        if let KeyIterator::Ascending(iterator) = self {
            return iterator;
        }

        panic!("Into ascending failed, reason: invalid key ascending iterator");
    }

    /// 将关键字迭代器转换为关键字倒序迭代器
    pub fn into_descending(self) -> KeyDescendingIterator<'a, K, V> {
        if let KeyIterator::Descending(iterator) = self {
            return iterator;
        }

        panic!("Into descending failed, reason: invalid key descending iterator");
    }
}

///
/// 关键字升序迭代器，保证关键字迭代器在使用时，被引用的关键字不会被释放
/// 当关键字迭代器回收时，自动减少对根节点的引用计数
///
pub struct KeyAscendingIterator<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    root:       Arc<Node<K, V>>,                    //根节点的共享引用
    stack:      Vec<(Node<K, V>, usize, bool)>,     //迭代节点栈
    index:      usize,                              //关键字偏移
    marker:     PhantomData<&'a ()>,
}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Iterator for KeyAscendingIterator<'a, K, V> {
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_key()
    }
}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> KeyAscendingIterator<'a, K, V> {
    // 构建从指定关键字开始的关键字升序迭代器
    // 如果指定关键字不存在，则从指定关键字的插入的位置开始迭代
    #[inline]
    fn new(tree: &CowBtreeMap<K, V>,
           root: Arc<Node<K, V>>,
           key: Option<&K>) -> Self {
        let mut stack = Vec::new();
        let index = if let Some(start_key) = key {
            //指定了开始关键字
            match query_by_iterate(root.as_ref(), start_key, &mut stack) {
                Err(index) => {
                    //指定关键字不存在
                    index
                },
                Ok(_last_value) => {
                    //指定关键字存在
                    if let Some((Node::Leaf(leaf), _, _)) = stack.last() {
                        //从叶节点中获取指定关键字对应的位置
                        leaf.index(start_key)
                    } else {
                        //查询栈的顶部一定是叶节点
                        panic!("Create key ascending iterator failed, key: {:?}, reason: require leaf",
                               start_key);
                    }
                }
            }
        } else {
            //未指定开始关键字
            if let Some(min_key) = tree.min_key() {
                //最小关键字存在
                let start_key = min_key.key();
                let _ = query_by_iterate(root.as_ref(), start_key, &mut stack);
            }

            0
        };

        KeyAscendingIterator {
            root,
            stack,
            index,
            marker: PhantomData,
        }
    }

    // 向后迭代关键字，成功返回关键字的只读引用，迭代结束返回空
    #[inline]
    fn next_key(&mut self) -> Option<&'a K> {
        let leaf_ref = if let Some((Node::Leaf(leaf), _, _)) = self.stack.last().as_ref() {
            //迭代器保证了关键字的只读引用，不会在迭代器回收前失效
            unsafe { &*Arc::as_ptr(leaf) }
        } else {
            //节点栈的顶部一定是叶节点
            return None;
        };

        if self.index >= leaf_ref.len() {
            //已经向后迭代完当前叶节点，则开始向后迭代下一个叶节点
            if self.next_node() {
                //向后迭代到下一个叶节点成功，则在下一个叶节点继续向后迭代关键字
                self.index = 0; //重置下一个叶节点的关键字偏移
                return self.next_key();
            } else {
                //已向后迭代完所有叶节点，则立即返回向后迭代结束
                return None;
            }
        } else {
            //继续在当前叶节点向后迭代关键字
            if let Some(pair) = leaf_ref.pairs.get(self.index) {
                //指定位置有关键字，则立即返回
                self.index += 1; //向后移动关键字偏移
                return Some(&pair.key);
            } else {
                //指定位置一定有关键字
                return None;
            }
        }
    }

    // 向后迭代节点
    #[inline]
    fn next_node(&mut self) -> bool {
        let _iterated_leaf = self.stack.pop().unwrap(); //弹出已迭代完成的叶节点

        let stack_len = self.stack.len();
        if stack_len == 0 {
            //当前根节点是叶节点，则立即返回向后迭代节点失败
            return false;
        }

        let mut is_require_next = false; //是否是需要向后迭代到当前非叶节点的右兄弟非叶节点
        loop {
            if let Some((Node::NonLeaf(non_leaf), superior_child_index, is_next_child)) = self.stack.pop() {
                if self.stack.len() > 0 && superior_child_index == non_leaf.len() - 1 {
                    //已向后迭代完当前非根非叶节点中的所有子节点，则尝试向后迭代到当前非根非叶节点的右兄弟非叶节点
                    is_require_next = true;
                    continue;
                }

                if is_require_next {
                    //可以向后迭代到当前非叶节点的右兄弟非叶节点
                    let next_superior_child_index = if !is_next_child {
                        //从当前非叶节点的最小键子对的前趋子节点向后迭代到后继子节点
                        superior_child_index
                    } else {
                        //从当前非叶节点的当前键子对向后迭代到下一个子节点
                        superior_child_index + 1
                    };

                    if self.stack.len() == 0 && next_superior_child_index >= non_leaf.len() {
                        //已向后迭代完根节点的所有子节点，则立即返回向后迭代节点失败
                        return false;
                    }

                    let non_leaf_right = non_leaf
                        .pairs[next_superior_child_index]
                        .next
                        .as_ref()
                        .unwrap()
                        .clone(); //从当前非叶节点的上级非叶节点获取当前非叶节点的右兄弟非叶节点
                    self.stack.push((Node::NonLeaf(non_leaf), next_superior_child_index, true)); //将弹出的当前非叶节点的上级非叶节点重新加入节点栈
                    self.stack.push((non_leaf_right, usize::MAX, false)); //将当前非叶节点的右兄弟非叶节点加入节点栈，已替换已迭代完成的当前非叶节点
                    is_require_next = false; //已向后迭代到当前非叶节点的右兄弟非叶节点，则重置
                    continue; //继续从当前非叶节点的右兄弟非叶节点开始，尝试向后迭代子节点
                }

                //可以继续向后迭代当前非叶节点中的下一个子节点
                let (next_superior_child_index, new_is_next_child, next_child)
                    = if superior_child_index == usize::MAX {
                    //刚从当前非叶节点向后迭代到当前非叶节点的右兄弟非叶节点，或刚从当前非叶节点向后迭代的下一个子节点
                    (0,
                     false,
                    non_leaf.pairs[0]
                        .prev
                        .as_ref()
                        .unwrap()
                        .clone())
                } else {
                    //仍然在当前非叶节点向后迭代下一个子节点
                    let next_superior_child_index = if !is_next_child {
                        //从当前非叶节点的最小键子对的前趋子节点向后迭代到后继子节点
                        superior_child_index
                    } else {
                        //从当前非叶节点的当前键子对向后迭代到下一个子节点
                        superior_child_index + 1
                    };

                    (next_superior_child_index,
                     true,
                     non_leaf.pairs[next_superior_child_index]
                         .next
                         .as_ref()
                         .unwrap()
                         .clone())
                };

                if next_child.is_leaf() {
                    //当前非叶节点的右兄弟非叶节点的下一个子节点是叶节点，则立即返回向后迭代节点成功
                    self.stack.push((Node::NonLeaf(non_leaf),
                                     next_superior_child_index,
                                     new_is_next_child)); //将弹出的当前非叶节点的右兄弟非叶节点重新加入节点栈
                    self.stack.push((next_child, 0, false)); //将下一个叶节点加入节点栈，已替换已迭代完成的当前叶节点
                    return true;
                } else {
                    //当前非叶节点的右兄弟非叶节点的下一个子节点是非叶节点，则继续向后迭代子节点
                    self.stack.push((Node::NonLeaf(non_leaf),
                                     next_superior_child_index,
                                     new_is_next_child)); //将弹出的当前非叶节点的右兄弟非叶节点重新加入节点栈
                    self.stack.push((next_child, usize::MAX, false)); //将下一个非叶节点加入节点堆栈
                    continue;
                }
            } else {
                //已向后迭代完所有子节点，则立即返回向后迭代节点失败
                return false;
            }
        }
    }
}


///
/// 关键字降序迭代器，保证关键字迭代器在使用时，被引用的关键字不会被释放
/// 当关键字迭代器回收时，自动减少对根节点的引用计数
///
pub struct KeyDescendingIterator<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    root:       Arc<Node<K, V>>,                //根节点的共享引用
    stack:      Vec<(Node<K, V>, usize, bool)>, //迭代节点栈
    index:      isize,                          //关键字偏移
    marker:     PhantomData<&'a ()>,
}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Iterator for KeyDescendingIterator<'a, K, V> {
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        self.prev_key()
    }
}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> KeyDescendingIterator<'a, K, V> {
    // 构建从指定关键字开始的关键字降序迭代器
    // 如果指定关键字不存在，则从指定关键字的插入的位置开始迭代
    #[inline]
    fn new(tree: &CowBtreeMap<K, V>,
           root: Arc<Node<K, V>>,
           key: Option<&K>) -> Self {
        let mut stack = Vec::new();
        let index = if let Some(start_key) = key {
            //指定了开始关键字
            match query_by_iterate(root.as_ref(), start_key, &mut stack) {
                Err(index) => {
                    //指定关键字不存在
                    index as isize - 1
                },
                Ok(_last_value) => {
                    //指定关键字存在
                    if let Some((Node::Leaf(leaf), _, _)) = stack.last() {
                        //从叶节点中获取指定关键字对应的位置
                        leaf.index(start_key) as isize
                    } else {
                        //查询栈的顶部一定是叶节点
                        panic!("Create key descenging iterator failed, key: {:?}, reason: require leaf",
                               start_key);
                    }
                }
            }
        } else {
            //未指定开始关键字
            if let Some(max_key) = tree.max_key() {
                //最大关键字存在
                let start_key = max_key.key();
                match query_by_iterate(root.as_ref(), start_key, &mut stack) {
                    Err(index) => {
                        //指定关键字不存在
                        index as isize - 1
                    },
                    Ok(_last_value) => {
                        //指定关键字存在
                        if let Some((Node::Leaf(leaf), _, _)) = stack.last() {
                            //从叶节点中获取指定关键字对应的位置
                            leaf.index(start_key) as isize
                        } else {
                            //查询栈的顶部一定是叶节点
                            panic!("Create key descenging iterator failed, key: {:?}, reason: require leaf",
                                   start_key);
                        }
                    }
                }
            } else {
                -1
            }
        };

        KeyDescendingIterator {
            root,
            stack,
            index,
            marker: PhantomData,
        }
    }

    // 向前迭代关键字，成功返回关键字的只读引用，迭代结束返回空
    #[inline]
    fn prev_key(&mut self) -> Option<&'a K> {
        let leaf_ref = if let Some((Node::Leaf(leaf), _, _)) = self.stack.last().as_ref() {
            //迭代器保证了关键字的只读引用，不会在迭代器回收前失效
            unsafe { &*Arc::as_ptr(leaf) }
        } else {
            //节点栈的顶部一定是叶节点
            return None;
        };

        if self.index < 0 {
            //已经向前迭代完当前叶节点，则开始向前迭代下一个叶节点
            if self.prev_node() {
                //向前迭代到下一个叶节点成功，则在下一个叶节点继续向前迭代关键字
                self.index = self
                    .stack
                    .last()
                    .unwrap()
                    .0
                    .as_leaf()
                    .unwrap()
                    .len() as isize - 1; //重置下一个叶节点的关键字偏移
                return self.prev_key();
            } else {
                //已向前迭代完所有叶节点，则立即返回向前迭代结束
                return None;
            }
        } else {
            //继续在当前叶节点向前迭代关键字
            if let Some(pair) = leaf_ref.pairs.get(self.index as usize) {
                //指定位置有关键字，则立即返回
                self.index -= 1; //向前移动关键字偏移
                return Some(&pair.key);
            } else {
                //指定位置一定有关键字
                return None;
            }
        }
    }

    // 向前迭代节点
    #[inline]
    fn prev_node(&mut self) -> bool {
        let _iterated_leaf = self.stack.pop().unwrap(); //弹出已迭代完成的叶节点

        let mut is_require_prev = false; //是否是需要向前迭代到当前非叶节点的左兄弟非叶节点
        loop {
            if let Some((Node::NonLeaf(non_leaf), superior_child_index, is_next_child)) = self.stack.pop() {
                if self.stack.len() > 0 && superior_child_index == 0 && !is_next_child {
                    //已向前迭代完当前非叶节点中的所有子节点，则尝试向前迭代到当前非叶节点的左兄弟非叶节点
                    is_require_prev = true;
                    continue;
                }

                if is_require_prev {
                    //可以向前迭代到当前非叶节点的左兄弟非叶节点
                    if self.stack.len() == 0 && superior_child_index == 0 && !is_next_child {
                        //已向前迭代完根节点的所有子节点，则立即返回向后迭代节点失败
                        return false;
                    }

                    let non_leaf_left = if superior_child_index == 0 && is_next_child {
                        //从当前非叶节点的上级非叶节点获取当前非叶节点的左兄弟非叶节点，当前位置为0，则获取当前位置的前趋子节点
                        let r = non_leaf
                            .pairs[superior_child_index]
                            .prev
                            .as_ref()
                            .unwrap()
                            .clone();
                        self.stack.push((Node::NonLeaf(non_leaf), 0, false)); //将弹出的当前非叶节点的上级非叶节点重新加入节点栈
                        r
                    } else {
                        //从当前非叶节点的上级非叶节点获取当前非叶节点的左兄弟非叶节点，当前位置大于0，则获取下一个位置的后继子节点
                        let prev_superior_child_index = superior_child_index - 1;
                        let r = non_leaf
                            .pairs[prev_superior_child_index]
                            .next
                            .as_ref()
                            .unwrap()
                            .clone();
                        self.stack.push((Node::NonLeaf(non_leaf), prev_superior_child_index, true)); //将弹出的当前非叶节点的上级非叶节点重新加入节点栈
                        r
                    };
                    self.stack.push((non_leaf_left.clone(), usize::MAX, true)); //将当前非叶节点的左兄弟非叶节点加入节点栈，已替换已迭代完成的当前非叶节点
                    is_require_prev = false; //已向前迭代到当前非叶节点的左兄弟非叶节点，则重置
                    continue; //继续从当前非叶节点的左兄弟非叶节点开始，尝试向前迭代子节点
                }

                //可以继续向前迭代当前非叶节点中的下一个子节点
                let (new_superior_child_index, new_is_next_child, prev_child)
                    = if superior_child_index == usize::MAX {
                    //刚从当前非叶节点向前迭代到当前非叶节点的左兄弟非叶节点，或刚从当前非叶节点向前迭代的下一个子节点
                    let index = non_leaf.len() - 1;
                    (index,
                     true,
                     non_leaf.pairs[index]
                         .next
                         .as_ref()
                         .unwrap()
                         .clone())
                } else {
                    //仍然在当前非叶节点向前迭代下一个子节点
                    if superior_child_index == 0 && is_next_child {
                        //当前位置为0，则迭代到当前位置的前趋子节点
                        (superior_child_index,
                         false,
                         non_leaf.pairs[superior_child_index]
                             .prev
                             .as_ref()
                             .unwrap()
                             .clone())
                    } else {
                        //当前位置大于0，则迭代到下一个位置的后继子节点
                        let new_superior_child_index = superior_child_index - 1;
                        (new_superior_child_index,
                         true,
                         non_leaf.pairs[new_superior_child_index]
                             .next
                             .as_ref()
                             .unwrap()
                             .clone())
                    }
                };

                if prev_child.is_leaf() {
                    //当前非叶节点的左兄弟非叶节点的下一个子节点是叶节点，则立即返回向前迭代节点成功
                    self.stack.push((Node::NonLeaf(non_leaf),
                                     new_superior_child_index,
                                     new_is_next_child)); //将弹出的当前非叶节点的左兄弟非叶节点重新加入节点栈
                    self.stack.push((prev_child, 0, false)); //将下一个叶节点加入节点栈，已替换已迭代完成的当前叶节点
                    return true;
                } else {
                    //当前非叶节点的左兄弟非叶节点的下一个子节点是非叶节点，则继续向前迭代子节点
                    self.stack.push((Node::NonLeaf(non_leaf),
                                     new_superior_child_index,
                                     new_is_next_child)); //将弹出的当前非叶节点的左兄弟非叶节点重新加入节点栈
                    self.stack.push((prev_child, usize::MAX, true)); //将下一个非叶节点加入节点堆栈
                    continue;
                }
            } else {
                //已向前迭代完所有子节点，则立即返回向前迭代节点失败
                return false;
            }
        }
    }
}

///
/// 键值对迭代器，保证键值对迭代器在使用时，被引用的键值对不会被释放
/// 当键值对迭代器回收时，自动减少对根节点的引用计数
///
pub enum KVPairIterator<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    Ascending(KVPairAscendingIterator<'a, K, V>),      //升序迭代器
    Descending(KVPairDescendingIterator<'a, K, V>),    //降序迭代器
}

unsafe impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Send for KVPairIterator<'a, K, V> {}
unsafe impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Sync for KVPairIterator<'a, K, V> {}

///
/// 键值对升序迭代器，保证键值对迭代器在使用时，被引用的键值对不会被释放
/// 当键值对迭代器回收时，自动减少对根节点的引用计数
///
pub struct KVPairAscendingIterator<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    root:       Arc<Node<K, V>>,                    //根节点的共享引用
    stack:      Vec<(Node<K, V>, usize, bool)>,     //迭代节点栈
    index:      usize,                              //键值对偏移
    marker:     PhantomData<&'a ()>,
}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Iterator for KVPairAscendingIterator<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_key_value()
    }
}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> KVPairAscendingIterator<'a, K, V> {
    // 构建从指定关键字开始的键值对升序迭代器
    // 如果指定关键字不存在，则从指定关键字的插入的位置开始迭代
    #[inline]
    fn new(tree: &CowBtreeMap<K, V>,
           root: Arc<Node<K, V>>,
           key: Option<&K>) -> Self {
        let mut stack = Vec::new();
        let index = if let Some(start_key) = key {
            //指定了开始关键字
            match query_by_iterate(root.as_ref(), start_key, &mut stack) {
                Err(index) => {
                    //指定关键字不存在
                    index
                },
                Ok(_last_value) => {
                    //指定关键字存在
                    if let Some((Node::Leaf(leaf), _, _)) = stack.last() {
                        //从叶节点中获取指定关键字对应的位置
                        leaf.index(start_key)
                    } else {
                        //查询栈的顶部一定是叶节点
                        panic!("Create key value pair ascending iterator failed, key: {:?}, reason: require leaf",
                               start_key);
                    }
                }
            }
        } else {
            //未指定开始关键字
            if let Some(min_key) = tree.min_key() {
                //最小关键字存在
                let start_key = min_key.key();
                let _ = query_by_iterate(root.as_ref(), start_key, &mut stack);
            }

            0
        };

        KVPairAscendingIterator {
            root,
            stack,
            index,
            marker: PhantomData,
        }
    }

    // 向后迭代键值对，成功返回关键字和值的只读引用，迭代结束返回空
    #[inline]
    fn next_key_value(&mut self) -> Option<(&'a K, &'a V)> {
        let leaf_ref = if let Some((Node::Leaf(leaf), _, _)) = self.stack.last().as_ref() {
            //迭代器保证了关键字的只读引用，不会在迭代器回收前失效
            unsafe { &*Arc::as_ptr(leaf) }
        } else {
            //节点栈的顶部一定是叶节点
            return None;
        };

        if self.index >= leaf_ref.len() {
            //已经向后迭代完当前叶节点，则开始向后迭代下一个叶节点
            if self.next_node() {
                //向后迭代到下一个叶节点成功，则在下一个叶节点继续向后迭代关键字
                self.index = 0; //重置下一个叶节点的关键字偏移
                return self.next_key_value();
            } else {
                //已向后迭代完所有叶节点，则立即返回向后迭代结束
                return None;
            }
        } else {
            //继续在当前叶节点向后迭代关键字
            if let Some(pair) = leaf_ref.pairs.get(self.index) {
                //指定位置有关键字，则立即返回
                self.index += 1; //向后移动关键字偏移
                return Some((&pair.key, &pair.value));
            } else {
                //指定位置一定有关键字
                return None;
            }
        }
    }

    // 向后迭代节点
    #[inline]
    fn next_node(&mut self) -> bool {
        let _iterated_leaf = self.stack.pop().unwrap(); //弹出已迭代完成的叶节点

        let stack_len = self.stack.len();
        if stack_len == 0 {
            //当前根节点是叶节点，则立即返回向后迭代节点失败
            return false;
        }

        let mut is_require_next = false; //是否是需要向后迭代到当前非叶节点的右兄弟非叶节点
        loop {
            if let Some((Node::NonLeaf(non_leaf), superior_child_index, is_next_child)) = self.stack.pop() {
                if self.stack.len() > 0 && superior_child_index == non_leaf.len() - 1 {
                    //已向后迭代完当前非叶节点中的所有子节点，则尝试向后迭代到当前非叶节点的右兄弟非叶节点
                    is_require_next = true;
                    continue;
                }

                if is_require_next {
                    //可以向后迭代到当前非叶节点的右兄弟非叶节点
                    let next_superior_child_index = if !is_next_child {
                        //从当前非叶节点的最小键子对的前趋子节点向后迭代到后继子节点
                        superior_child_index
                    } else {
                        //从当前非叶节点的当前键子对向后迭代到下一个子节点
                        superior_child_index + 1
                    };

                    if self.stack.len() == 0 && next_superior_child_index >= non_leaf.len() {
                        //已向后迭代完根节点的所有子节点，则立即返回向后迭代节点失败
                        return false;
                    }

                    let non_leaf_right = non_leaf
                        .pairs[next_superior_child_index]
                        .next
                        .as_ref()
                        .unwrap()
                        .clone(); //从当前非叶节点的上级非叶节点获取当前非叶节点的右兄弟非叶节点
                    self.stack.push((Node::NonLeaf(non_leaf), next_superior_child_index, true)); //将弹出的当前非叶节点的上级非叶节点重新加入节点栈
                    self.stack.push((non_leaf_right, usize::MAX, false)); //将当前非叶节点的右兄弟非叶节点加入节点栈，已替换已迭代完成的当前非叶节点
                    is_require_next = false; //已向后迭代到当前非叶节点的右兄弟非叶节点，则重置
                    continue; //继续从当前非叶节点的右兄弟非叶节点开始，尝试向后迭代子节点
                }

                //可以继续向后迭代当前非叶节点中的下一个子节点
                let (next_superior_child_index, new_is_next_child, next_child) = if superior_child_index == usize::MAX {
                    //刚从当前非叶节点向后迭代到当前非叶节点的右兄弟非叶节点，或刚从当前非叶节点向后迭代的下一个子节点
                    (0,
                     false,
                     non_leaf.pairs[0]
                         .prev
                         .as_ref()
                         .unwrap()
                         .clone())
                } else {
                    //仍然在当前非叶节点向后迭代下一个子节点
                    let next_superior_child_index = if !is_next_child {
                        //从当前非叶节点的最小键子对的前趋子节点向后迭代到后继子节点
                        superior_child_index
                    } else {
                        //从当前非叶节点的当前键子对向后迭代到下一个子节点
                        superior_child_index + 1
                    };

                    (next_superior_child_index,
                     true,
                     non_leaf.pairs[next_superior_child_index]
                         .next
                         .as_ref()
                         .unwrap()
                         .clone())
                };

                if next_child.is_leaf() {
                    //当前非叶节点的右兄弟非叶节点的下一个子节点是叶节点，则立即返回向后迭代节点成功
                    self.stack.push((Node::NonLeaf(non_leaf),
                                     next_superior_child_index,
                                     new_is_next_child)); //将弹出的当前非叶节点的右兄弟非叶节点重新加入节点栈
                    self.stack.push((next_child, 0, false)); //将下一个叶节点加入节点栈，已替换已迭代完成的当前叶节点
                    return true;
                } else {
                    //当前非叶节点的右兄弟非叶节点的下一个子节点是非叶节点，则继续向后迭代子节点
                    self.stack.push((Node::NonLeaf(non_leaf),
                                     next_superior_child_index,
                                     new_is_next_child)); //将弹出的当前非叶节点的右兄弟非叶节点重新加入节点栈
                    self.stack.push((next_child, usize::MAX, false)); //将下一个非叶节点加入节点堆栈
                    continue;
                }
            } else {
                //已向后迭代完所有子节点，则立即返回向后迭代节点失败
                return false;
            }
        }
    }
}

///
/// 键值对降序迭代器，保证键值对迭代器在使用时，被引用的键值对不会被释放
/// 当键值对迭代器回收时，自动减少对根节点的引用计数
///
pub struct KVPairDescendingIterator<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    root:       Arc<Node<K, V>>,                //根节点的共享引用
    stack:      Vec<(Node<K, V>, usize, bool)>, //迭代节点栈
    index:      isize,                          //键值对偏移
    marker:     PhantomData<&'a ()>,
}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Iterator for KVPairDescendingIterator<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.prev_key_value()
    }
}

impl<
    'a,
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> KVPairDescendingIterator<'a, K, V> {
    // 构建从指定关键字开始的键值对降序迭代器
    // 如果指定关键字不存在，则从指定关键字的插入的位置开始迭代
    #[inline]
    fn new(tree: &CowBtreeMap<K, V>,
           root: Arc<Node<K, V>>,
           key: Option<&K>) -> Self {
        let mut stack = Vec::new();
        let index = if let Some(start_key) = key {
            //指定了开始关键字
            match query_by_iterate(root.as_ref(), start_key, &mut stack) {
                Err(index) => {
                    //指定关键字不存在
                    index as isize - 1
                },
                Ok(_last_value) => {
                    //指定关键字存在
                    if let Some((Node::Leaf(leaf), _, _)) = stack.last() {
                        //从叶节点中获取指定关键字对应的位置
                        leaf.index(start_key) as isize
                    } else {
                        //查询栈的顶部一定是叶节点
                        panic!("Create key descenging iterator failed, key: {:?}, reason: require leaf",
                               start_key);
                    }
                }
            }
        } else {
            //未指定开始关键字
            if let Some(max_key) = tree.max_key() {
                //最大关键字存在
                let start_key = max_key.key();
                match query_by_iterate(root.as_ref(), start_key, &mut stack) {
                    Err(index) => {
                        //指定关键字不存在
                        index as isize - 1
                    },
                    Ok(_last_value) => {
                        //指定关键字存在
                        if let Some((Node::Leaf(leaf), _, _)) = stack.last() {
                            //从叶节点中获取指定关键字对应的位置
                            leaf.index(start_key) as isize
                        } else {
                            //查询栈的顶部一定是叶节点
                            panic!("Create key descenging iterator failed, key: {:?}, reason: require leaf",
                                   start_key);
                        }
                    }
                }
            } else {
                -1
            }
        };

        KVPairDescendingIterator {
            root,
            stack,
            index,
            marker: PhantomData,
        }
    }

    // 向前迭代键值对，成功返回关键字和值的只读引用，迭代结束返回空
    #[inline]
    fn prev_key_value(&mut self) -> Option<(&'a K, &'a V)> {
        let leaf_ref = if let Some((Node::Leaf(leaf), _, _)) = self.stack.last().as_ref() {
            //迭代器保证了关键字的只读引用，不会在迭代器回收前失效
            unsafe { &*Arc::as_ptr(leaf) }
        } else {
            //节点栈的顶部一定是叶节点
            return None;
        };

        if self.index < 0 {
            //已经向前迭代完当前叶节点，则开始向前迭代下一个叶节点
            if self.prev_node() {
                //向前迭代到下一个叶节点成功，则在下一个叶节点继续向前迭代关键字
                self.index = self
                    .stack
                    .last()
                    .unwrap()
                    .0
                    .as_leaf()
                    .unwrap()
                    .len() as isize - 1; //重置下一个叶节点的关键字偏移
                return self.prev_key_value();
            } else {
                //已向前迭代完所有叶节点，则立即返回向前迭代结束
                return None;
            }
        } else {
            //继续在当前叶节点向前迭代关键字
            if let Some(pair) = leaf_ref.pairs.get(self.index as usize) {
                //指定位置有关键字，则立即返回
                self.index -= 1; //向前移动关键字偏移
                return Some((&pair.key, &pair.value));
            } else {
                //指定位置一定有关键字
                return None;
            }
        }
    }

    // 向前迭代节点
    #[inline]
    fn prev_node(&mut self) -> bool {
        let _iterated_leaf = self.stack.pop().unwrap(); //弹出已迭代完成的叶节点

        let mut is_require_prev = false; //是否是需要向前迭代到当前非叶节点的左兄弟非叶节点
        loop {
            if let Some((Node::NonLeaf(non_leaf), superior_child_index, is_next_child)) = self.stack.pop() {
                if self.stack.len() > 0 && superior_child_index == 0 && !is_next_child {
                    //已向前迭代完当前非叶节点中的所有子节点，则尝试向前迭代到当前非叶节点的左兄弟非叶节点
                    is_require_prev = true;
                    continue;
                }

                if is_require_prev {
                    //可以向前迭代到当前非叶节点的左兄弟非叶节点
                    if self.stack.len() == 0 && superior_child_index == 0 && !is_next_child {
                        //已向前迭代完根节点的所有子节点，则立即返回向后迭代节点失败
                        return false;
                    }

                    let non_leaf_left = if superior_child_index == 0 && is_next_child {
                        //从当前非叶节点的上级非叶节点获取当前非叶节点的左兄弟非叶节点，当前位置为0，则获取当前位置的前趋子节点
                        let r = non_leaf
                            .pairs[superior_child_index]
                            .prev
                            .as_ref()
                            .unwrap()
                            .clone();
                        self.stack.push((Node::NonLeaf(non_leaf), 0, false)); //将弹出的当前非叶节点的上级非叶节点重新加入节点栈
                        r
                    } else {
                        //从当前非叶节点的上级非叶节点获取当前非叶节点的左兄弟非叶节点，当前位置大于0，则获取下一个位置的后继子节点
                        let prev_superior_child_index = superior_child_index
                            .checked_sub(1)
                            .unwrap_or(0);
                        let r = non_leaf
                            .pairs[prev_superior_child_index]
                            .next
                            .as_ref()
                            .unwrap()
                            .clone();
                        self.stack.push((Node::NonLeaf(non_leaf), prev_superior_child_index, true)); //将弹出的当前非叶节点的上级非叶节点重新加入节点栈
                        r
                    };
                    self.stack.push((non_leaf_left.clone(), usize::MAX, true)); //将当前非叶节点的左兄弟非叶节点加入节点栈，已替换已迭代完成的当前非叶节点
                    is_require_prev = false; //已向前迭代到当前非叶节点的左兄弟非叶节点，则重置
                    continue; //继续从当前非叶节点的左兄弟非叶节点开始，尝试向前迭代子节点
                }

                //可以继续向前迭代当前非叶节点中的下一个子节点
                let (new_superior_child_index, new_is_next_child, prev_child) = if superior_child_index == usize::MAX {
                    //刚从当前非叶节点向前迭代到当前非叶节点的左兄弟非叶节点，或刚从当前非叶节点向前迭代的下一个子节点
                    let index = non_leaf.len() - 1;
                    (index,
                     true,
                     non_leaf.pairs[index]
                         .next
                         .as_ref()
                         .unwrap()
                         .clone())
                } else {
                    //仍然在当前非叶节点向前迭代下一个子节点
                    if superior_child_index == 0 {
                        //当前位置为0，则迭代到当前位置的前趋子节点
                        (superior_child_index,
                         false,
                         non_leaf.pairs[superior_child_index]
                             .prev
                             .as_ref()
                             .unwrap()
                             .clone())
                    } else {
                        //当前位置大于0，则迭代到下一个位置的后继子节点
                        let new_superior_child_index = superior_child_index
                            .checked_sub(1)
                            .unwrap_or(0);
                        (new_superior_child_index,
                         true,
                         non_leaf.pairs[new_superior_child_index]
                             .next
                             .as_ref()
                             .unwrap()
                             .clone())
                    }
                };

                if prev_child.is_leaf() {
                    //当前非叶节点的左兄弟非叶节点的下一个子节点是叶节点，则立即返回向前迭代节点成功
                    self.stack.push((Node::NonLeaf(non_leaf),
                                     new_superior_child_index,
                                     new_is_next_child)); //将弹出的当前非叶节点的左兄弟非叶节点重新加入节点栈
                    self.stack.push((prev_child, 0, false)); //将下一个叶节点加入节点栈，已替换已迭代完成的当前叶节点
                    return true;
                } else {
                    //当前非叶节点的左兄弟非叶节点的下一个子节点是非叶节点，则继续向前迭代子节点
                    self.stack.push((Node::NonLeaf(non_leaf),
                                     new_superior_child_index,
                                     new_is_next_child)); //将弹出的当前非叶节点的左兄弟非叶节点重新加入节点栈
                    self.stack.push((prev_child, usize::MAX, true)); //将下一个非叶节点加入节点堆栈
                    continue;
                }
            } else {
                //已向前迭代完所有子节点，则立即返回向前迭代节点失败
                return false;
            }
        }
    }
}

// 迭代用查询
fn query_by_iterate<'a: 'b, 'b, K, V>(mut node: &'a Node<K, V>,
                                      key: &'a K,
                                      stack: &'b mut Vec<(Node<K, V>, usize, bool)>) -> Result<&'a V, usize>
    where K: Ord + Debug + Clone + Send + 'static,
          V: Debug + Clone + Send + 'static {
    loop {
        let node_copy = node.clone(); //为后续的读操作复制节点的共享引用

        let (child_index, is_next_child) = if node.is_leaf() {
            //叶节点，则查询指定关键字的值
            stack.push((node_copy, 0, false)); //将当前叶节点的上级非叶节点复制和当前节点在上级非叶节点的键子对列表中的位置加入查询栈
            return query_leaf(node.as_leaf().unwrap(), key);
        } else {
            //非叶节点，则获取下一个子节点，并继续查询
            let r = query_non_leaf(node.as_non_leaf().unwrap(), key);
            node = r.0;
            (r.1, r.2)
        };
        stack.push((node_copy, child_index, is_next_child)); //将当前节点的上级非叶节点复制和当前节点在上级非叶节点的键子对列表中的位置加入查询栈
    }
}

///
/// 手动提交对树的修改
///
pub struct ManualCommit<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> {
    tree:       CowBtreeMap<K, V>,  //树
    root:       Arc<Node<K, V>>,    //根节点的共享引用
    new_depth:  usize,              //修改树以后的新深度
    new_root:   Arc<Node<K, V>>,    //修改树以后的新根节点
}

unsafe impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Send for ManualCommit<K, V> {}
unsafe impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> Sync for ManualCommit<K, V> {}

impl<
    K: Ord + Debug + Clone + Send + 'static,
    V: Debug + Clone + Send + 'static,
> ManualCommit<K, V> {
    // 构建手动提交对树的修改
    fn new(tree: CowBtreeMap<K, V>,
           root: Arc<Node<K, V>>,
           (new_depth, new_root): &(usize, Arc<Node<K, V>>)) -> Self {
        ManualCommit {
            tree,
            root,
            new_depth: *new_depth,
            new_root: new_root.clone(),
        }
    }

    /// 提交对树的所有修改，提交超时则返回自身
    pub fn commit(self, timeout: Option<u128>) -> Result<(), Self> {
        if let None = safety_modify_tree(&self.tree,
                                         (self.new_depth.clone(), self.new_root.clone()),
                                         timeout) {
            //提交超时
            return Err(self);
        }

        //提交成功
        Ok(())
    }
}

// 自旋
#[inline]
fn spin(mut len: u32) -> u32 {
    if len < 1 {
        len = 1;
    } else if len > 10 {
        len = 10;
    }

    for _ in 0..(1 << len) {
        spin_loop()
    }

    len + 1
}

