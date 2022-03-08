use std::cmp::{max, min};

use itertools::Itertools;
use rayon::prelude::*;

#[derive(Debug)]
#[derive(Clone, Copy, PartialEq)]
pub struct Interval {
    pub start: i32,
    pub end: i32,
}

impl Interval {
    pub fn new(start: i32, end: i32) -> Self {
        Self {
            start,
            end,
        }
    }


    pub fn toPoints(&self) -> Vec<i32> {
        //let mut out: Vec<i32> = vec![];
        return (self.start..self.end + 1).into_iter().collect();
        //return out;

        // let mut out:Vec<i32> = vec![];
        // for i in self.start..self.end+1{
        //     out.push(i as i32 );
        // }
        // return out;
    }

    pub fn zip(&self, pid: i32) -> Vec<(i32, i32)> {
        return (self.start..self.end + 1).into_iter().map(|i| (pid, i)).collect_vec();

        // for i in  (self.start..self.end+1){
        //     out.push((pid,i));
        // }
        // return out;
    }

    // pub fn zip_op(&self,pid:i32, op:i32) -> Vec<(i32,i32)>{
    //     let mut out:Vec<(i32,i32)> = vec![];
    //     for i in self.start..self.end+1{
    //         if (i as i32 + op ) >= (self.start as i32) && (i as i32 +op) as i32<= (self.end as i32 ) {
    //             out.push((pid, (i as i32 +op) as i32));
    //         }
    //     }
    //     return out;
    // }

    pub fn zip_vec(&self, pid: &Vec<i32>) -> Vec<Vec<(i32, i32)>> {
        //x.zip_vec(y) -> (y,x)
        let out: Vec<Vec<(i32, i32)>> = (self.start..self.end + 1).into_iter().map(|x| pid.iter().map(|y| (*y, x)).collect()).collect();
        return out;


        // for i in self.start..self.end+1 {
        //     let mut t: Vec<(i32, i32)> = vec![];
        //     for j in pid {
        //        t.push((*j, i));
        //     }
        //     out.push(t);
        // }
        // return out;
    }

    pub fn zip_two_points(&self, pid: &Vec<i32>) -> Vec<(i32, i32)> {
        //x.zip_vec(y) -> (y,x)
        let mut out = vec![];
        for i in self.start..self.end + 1 {
            for j in pid {
                out.push((*j, i));
            }
        }
        return out;
    }


    pub fn isBefore(&self, other: &Interval) -> bool {
        return self.start < other.start && self.end < other.end;
    }

    pub fn leftBefore(&self, other: &Interval) -> Option<Interval> {
        if self.start > other.start {
            return None
        }
        if self.end < other.start{
            return Some(*self)
        }
        if self.end > other.start{
            return Some(Interval{start:self.start, end:other.start})
        }
        return None;

    }

    pub fn isPrev(&self, other: &Interval) -> bool {
        return self.start < other.end;
    }
    pub fn isNext(&self, other: &Interval) -> bool {
        return self.end > other.start;
    }


    pub fn prev(&self, other: &Interval) -> Option<Interval>
    {
        // Compute the intresction of two interval x and y, where x is prev of y x.prev(y)
        let start = max(self.start, max(other.start - 1, 0));
        let end = min(self.end, max(other.end - 1, 0));
        if start > 0 && end > 0 && end >= start {
            return Some(Interval { start, end });
        }
        return None;
    }

    pub fn next(&self, other: &Interval) -> Option<Interval>
    {
        // Compute the intresction of two interval x and y, where x is prev of y x.next(y)
        let start = max(self.start, other.start + 1);
        let end = min(self.end, other.end + 1);
        if start > 0 && end > 0 && end >= start {
            return Some(Interval { start, end });
        }
        return None;
    }


    pub fn overlap(&self, other: &Interval) -> bool {
        let flag =
            self.start <= other.end && self.end >= other.start;
        flag
    }

    pub fn intersect(&self, other: &Interval) -> Interval {
        let start = max(self.start, other.start);
        let end = min(self.end, other.end);
        return Interval { start, end };
    }


    // pub fn prev(&self) -> Interval{
    //     let min = self.start;
    //     return Interval{start, end:end-1}
    // }
}