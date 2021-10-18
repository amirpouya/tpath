#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::interval::Interval;

    #[test]
    fn test_interval_overlap() {
        assert_eq!(Interval{start: 3,end: 3}.overlap(&Interval{start: 3,end: 3}), true);
        assert_eq!(Interval{start: 3,end: 3}.overlap(&Interval{start: 3,end: 6}), true);
        assert_eq!(Interval{start: 6,end: 7}.overlap(&Interval{start: 5,end: 6}), true);

        assert_eq!(Interval{start: 3,end: 6}.overlap(&Interval{start: 3,end: 3}), true);
        assert_eq!(Interval{start: 5,end: 6}.overlap(&Interval{start: 6,end: 7}), true);

        assert_eq!(Interval{start: 3,end: 3}.overlap(&Interval{start: 5,end: 6}), false);
        assert_eq!(Interval{start: 7,end: 8}.overlap(&Interval{start: 5,end: 6}), false);

        assert_eq!(Interval{start: 5,end: 6}.overlap(&Interval{start: 3,end: 3}), false);
        assert_eq!(Interval{start: 5,end: 6}.overlap(&Interval{start: 7,end: 8}), false);

        assert_eq!(Interval{start: 5,end: 6}.overlap(&Interval{start: 9,end: 9}), false);

    }

    #[test]
    fn test_interval_intersect() {
        assert_eq!(Interval{start: 3,end: 3}.intersect(&Interval{start: 3,end: 3}), Interval{start: 3,end: 3});
        assert_eq!(Interval{start: 3,end: 3}.intersect(&Interval{start: 3,end: 6}), Interval{start: 3,end: 3});
        assert_eq!(Interval{start: 6,end: 7}.intersect(&Interval{start: 5,end: 6}), Interval{start: 6,end: 6});
        assert_eq!(Interval{start: 1,end: 9}.intersect(&Interval{start: 3,end: 3}), Interval{start: 3,end: 3});
        assert_eq!(Interval{start: 1,end: 9}.intersect(&Interval{start: 3,end: 3}), Interval{start: 3,end: 3});
        assert_eq!(Interval{start: 3,end: 6}.intersect(&Interval{start: 3,end: 3}), Interval{start: 3,end: 3});
        assert_eq!(Interval{start: 1,end: 9}.intersect(&Interval{start: 1,end: 4}), Interval{start: 1,end: 4});
        assert_eq!(Interval{start: 5,end: 6}.intersect(&Interval{start: 6,end: 7}), Interval{start: 6,end: 6});
        assert_eq!(Interval{start: 6,end: 7}.intersect(&Interval{start: 5,end: 6}), Interval{start: 6,end: 6});
    }
    
    #[test]
    fn test_interval_prev(){
        assert_eq!(Interval{start: 1,end: 1}.prev(&Interval { start: 6, end: 9 }), None);
        assert_eq!(Interval{start: 10,end: 12}.prev(&Interval { start: 6, end: 9 }), None);
        assert_eq!(Interval{start: 1,end: 100}.prev(&Interval { start: 6, end: 9 }), Some(Interval{start:5,end:8}));

        assert_eq!(Interval{start: 1,end: 100}.prev(&Interval { start: 1, end: 9 }), Some(Interval{start:1,end:8}));
        assert_eq!(Interval{start: 1,end: 100}.prev(&Interval { start: 1, end: 1 }), None);

        assert_eq!(Interval{start: 1,end: 100}.prev(&Interval { start: 9, end: 9}), Some(Interval{start:8,end:8}));
        assert_eq!(Interval{start: 8,end: 9}.prev(&Interval { start: 9, end: 9}), Some(Interval{start:8,end:8}));
        assert_eq!(Interval{start: 7,end: 9}.prev(&Interval { start: 9, end: 9}), Some(Interval{start:8,end:8}));


        assert_eq!(Interval{start: 7,end: 13}.prev(&Interval { start: 6, end: 9 }), Some(Interval{start: 7,end: 8}));

    }

}
