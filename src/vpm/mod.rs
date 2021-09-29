

///
/// 页面id，是指向虚拟块地址的指针
/// 页面id分配后，将不会改变，只会在确认释放后被回收
///
#[derive(Debug, Clone)]
pub struct PageId(u64);

impl PageId {
    /// 创建页面id
    pub(crate) fn new(id: u64) -> Self {
        PageId(id)
    }
}