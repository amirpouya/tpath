#[cfg(test)]
mod tests {
    use crate::interval::Interval;

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_interval_overlap() {
        assert_eq!(Interval { start: 3, end: 3 }.overlap(&Interval { start: 3, end: 3 }), true);
        assert_eq!(Interval { start: 3, end: 3 }.overlap(&Interval { start: 3, end: 6 }), true);
        assert_eq!(Interval { start: 6, end: 7 }.overlap(&Interval { start: 5, end: 6 }), true);

        assert_eq!(Interval { start: 3, end: 6 }.overlap(&Interval { start: 3, end: 3 }), true);
        assert_eq!(Interval { start: 5, end: 6 }.overlap(&Interval { start: 6, end: 7 }), true);

        assert_eq!(Interval { start: 3, end: 3 }.overlap(&Interval { start: 5, end: 6 }), false);
        assert_eq!(Interval { start: 7, end: 8 }.overlap(&Interval { start: 5, end: 6 }), false);

        assert_eq!(Interval { start: 5, end: 6 }.overlap(&Interval { start: 3, end: 3 }), false);
        assert_eq!(Interval { start: 5, end: 6 }.overlap(&Interval { start: 7, end: 8 }), false);

        assert_eq!(Interval { start: 5, end: 6 }.overlap(&Interval { start: 9, end: 9 }), false);
    }

    #[test]
    fn test_interval_intersect() {
        assert_eq!(Interval { start: 3, end: 3 }.intersect(&Interval { start: 3, end: 3 }), Interval { start: 3, end: 3 });
        assert_eq!(Interval { start: 3, end: 3 }.intersect(&Interval { start: 3, end: 6 }), Interval { start: 3, end: 3 });
        assert_eq!(Interval { start: 6, end: 7 }.intersect(&Interval { start: 5, end: 6 }), Interval { start: 6, end: 6 });
        assert_eq!(Interval { start: 1, end: 9 }.intersect(&Interval { start: 3, end: 3 }), Interval { start: 3, end: 3 });
        assert_eq!(Interval { start: 1, end: 9 }.intersect(&Interval { start: 3, end: 3 }), Interval { start: 3, end: 3 });
        assert_eq!(Interval { start: 3, end: 6 }.intersect(&Interval { start: 3, end: 3 }), Interval { start: 3, end: 3 });
        assert_eq!(Interval { start: 1, end: 9 }.intersect(&Interval { start: 1, end: 4 }), Interval { start: 1, end: 4 });
        assert_eq!(Interval { start: 5, end: 6 }.intersect(&Interval { start: 6, end: 7 }), Interval { start: 6, end: 6 });
        assert_eq!(Interval { start: 6, end: 7 }.intersect(&Interval { start: 5, end: 6 }), Interval { start: 6, end: 6 });
    }

    #[test]
    fn test_interval_prev() {
        assert_eq!(Interval { start: 1, end: 1 }.prev(&Interval { start: 6, end: 9 }), None);
        assert_eq!(Interval { start: 10, end: 12 }.prev(&Interval { start: 6, end: 9 }), None);
        assert_eq!(Interval { start: 1, end: 100 }.prev(&Interval { start: 6, end: 9 }), Some(Interval { start: 5, end: 8 }));

        assert_eq!(Interval { start: 1, end: 100 }.prev(&Interval { start: 1, end: 9 }), Some(Interval { start: 1, end: 8 }));
        assert_eq!(Interval { start: 1, end: 100 }.prev(&Interval { start: 1, end: 1 }), None);

        assert_eq!(Interval { start: 1, end: 100 }.prev(&Interval { start: 9, end: 9 }), Some(Interval { start: 8, end: 8 }));
        assert_eq!(Interval { start: 8, end: 9 }.prev(&Interval { start: 9, end: 9 }), Some(Interval { start: 8, end: 8 }));
        assert_eq!(Interval { start: 7, end: 9 }.prev(&Interval { start: 9, end: 9 }), Some(Interval { start: 8, end: 8 }));


        assert_eq!(Interval { start: 7, end: 13 }.prev(&Interval { start: 6, end: 9 }), Some(Interval { start: 7, end: 8 }));

        assert_eq!(Interval { start: 3, end: 8 }.prev(&Interval { start: 9, end: 9 }), Some(Interval { start: 8, end: 8 }));
        assert_eq!(Interval { start: 3, end: 8 }.prev(&Interval { start: 8, end: 8 }), Some(Interval { start: 7, end: 7 }));
        assert_eq!(Interval { start: 3, end: 8 }.prev(&Interval { start: 7, end: 7 }), Some(Interval { start: 7, end: 7 }));
        assert_eq!(Interval { start: 3, end: 8 }.prev(&Interval { start: 8, end: 8 }), Some(Interval { start: 7, end: 7 }));
    }

    #[test]
    fn test_interval_next() {
        assert_eq!(Interval { start: 1, end: 1 }.next(&Interval { start: 6, end: 9 }), None);
        assert_eq!(Interval { start: 10, end: 12 }.next(&Interval { start: 6, end: 9 }), Some(Interval { start: 10, end: 10 }));
        assert_eq!(Interval { start: 1, end: 100 }.next(&Interval { start: 6, end: 9 }), Some(Interval { start: 7, end: 10 }));

        assert_eq!(Interval { start: 1, end: 100 }.next(&Interval { start: 1, end: 9 }), Some(Interval { start: 2, end: 10 }));
        assert_eq!(Interval { start: 1, end: 100 }.next(&Interval { start: 100, end: 100 }), None);

        assert_eq!(Interval { start: 1, end: 100 }.next(&Interval { start: 9, end: 9 }), Some(Interval { start: 10, end: 10 }));
        assert_eq!(Interval { start: 8, end: 9 }.next(&Interval { start: 9, end: 9 }), None);
        assert_eq!(Interval { start: 7, end: 9 }.next(&Interval { start: 9, end: 9 }), None);


        assert_eq!(Interval { start: 7, end: 13 }.next(&Interval { start: 6, end: 9 }), Some(Interval { start: 7, end: 10 }));

        // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 9, end: 9 }), Some(Interval{start: 8,end: 8}));
        // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 8, end: 8 }), Some(Interval{start: 7,end: 7}));
        // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 7, end: 7 }), Some(Interval{start: 7,end: 7}));
        // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 8, end: 8 }), Some(Interval{start: 7,end: 7}));
    }


    #[test]
    fn test_interval_isnext() {
        assert_eq!(Interval { start: 9, end: 9 }.isNext(&Interval { start: 5, end: 6 }), true);
        // assert_eq!(Interval{start: 10,end: 12}.next(&Interval { start: 6, end: 9 }), Some(Interval{start:10,end:10}));
        // assert_eq!(Interval{start: 1,end: 100}.next(&Interval { start: 6, end: 9 }), Some(Interval{start:7,end:10}));
        //
        // assert_eq!(Interval{start: 1,end: 100}.next(&Interval { start: 1, end: 9 }), Some(Interval{start:2,end:10}));
        // assert_eq!(Interval{start: 1,end: 100}.next(&Interval { start: 100, end: 100 }), None);
        //
        // assert_eq!(Interval{start: 1,end: 100}.next(&Interval { start: 9, end: 9}), Some(Interval{start:10,end:10}));
        // assert_eq!(Interval{start: 8,end: 9}.next(&Interval { start: 9, end: 9}), None);
        // assert_eq!(Interval{start: 7,end: 9}.next(&Interval { start: 9, end: 9}), None);
        //
        //
        // assert_eq!(Interval{start: 7,end: 13}.next(&Interval { start: 6, end: 9 }), Some(Interval{start: 7,end: 10}));
        //
        // // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 9, end: 9 }), Some(Interval{start: 8,end: 8}));
        // // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 8, end: 8 }), Some(Interval{start: 7,end: 7}));
        // // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 7, end: 7 }), Some(Interval{start: 7,end: 7}));
        // // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 8, end: 8 }), Some(Interval{start: 7,end: 7}));
        //
    }


    #[test]
    fn test_interval_queality() {
        assert_eq!((Interval { start: 9, end: 9 } == Interval { start: 9, end: 9 }), true);
        // assert_eq!(Interval{start: 10,end: 12}.next(&Interval { start: 6, end: 9 }), Some(Interval{start:10,end:10}));
        // assert_eq!(Interval{start: 1,end: 100}.next(&Interval { start: 6, end: 9 }), Some(Interval{start:7,end:10}));
        //
        // assert_eq!(Interval{start: 1,end: 100}.next(&Interval { start: 1, end: 9 }), Some(Interval{start:2,end:10}));
        // assert_eq!(Interval{start: 1,end: 100}.next(&Interval { start: 100, end: 100 }), None);
        //
        // assert_eq!(Interval{start: 1,end: 100}.next(&Interval { start: 9, end: 9}), Some(Interval{start:10,end:10}));
        // assert_eq!(Interval{start: 8,end: 9}.next(&Interval { start: 9, end: 9}), None);
        // assert_eq!(Interval{start: 7,end: 9}.next(&Interval { start: 9, end: 9}), None);
        //
        //
        // assert_eq!(Interval{start: 7,end: 13}.next(&Interval { start: 6, end: 9 }), Some(Interval{start: 7,end: 10}));
        //
        // // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 9, end: 9 }), Some(Interval{start: 8,end: 8}));
        // // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 8, end: 8 }), Some(Interval{start: 7,end: 7}));
        // // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 7, end: 7 }), Some(Interval{start: 7,end: 7}));
        // // assert_eq!(Interval{start: 3,end: 8}.prev(&Interval { start: 8, end: 8 }), Some(Interval{start: 7,end: 7}));
        //
    }
}
