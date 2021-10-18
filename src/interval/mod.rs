use std::cmp::{min, max};

#[derive(Debug)]
#[derive(Clone, Copy, PartialEq)]
pub struct Interval{
    pub start: usize,
    pub end: usize
}

impl Interval{
    pub fn new(start: usize, end:usize) -> Self {
        Self {
            start,end
        }
    }


    pub fn toPoints(&self) -> Vec<usize>{
        let mut out:Vec<usize> = vec![];
        for i in self.start..self.end+1{
            out.push(i);
        }
        return out;
    }

    pub fn zip(&self,pid:usize) -> Vec<(usize,usize)>{
        let mut out:Vec<(usize,usize)> = vec![];
        for i in self.start..self.end+1{
            out.push((pid,i));
        }
        return out;
    }

    pub fn zip_op(&self,pid:usize, op:i8) -> Vec<(usize,usize)>{
        let mut out:Vec<(usize,usize)> = vec![];
        for i in self.start..self.end+1{
            if (i as i8 + op ) >= (self.start as i8) && (i as i8 +op) as i8<= (self.end as i8 ) {
                out.push((pid, (i as i8 +op) as usize));
            }
        }
        return out;
    }

    pub fn zip_vec(&self,pid:&Vec<usize>) -> Vec<Vec<(usize,usize)>>{
        let mut out:Vec<Vec<(usize,usize)>> = vec![];
        for i in self.start..self.end+1 {
            let mut t: Vec<(usize, usize)> = vec![];
            for j in pid {
               t.push((*j, i));
            }
            out.push(t);
        }
        return out;
    }

    // pub fn zip_vec_opt(&self,pid:&Vec<usize>,op:&Vec<i8>) -> Vec<Vec<(usize,usize)>>{
    //     let mut out:Vec<Vec<(usize,usize)>> = vec![];
    //     for i in self.start..self.end+1 {
    //         let mut t: Vec<(usize, usize)> = vec![];
    //         for (x,j) in pid.iter().enumerate() {
    //             if i + op[x] >= self.start && i+op[x] <= self.end {
    //                 t.push((*j, i +op[x]));
    //             }
    //         }
    //         out.push(t);
    //     }
    //     return out;
    // }


    pub fn isBefore(&self,other:&Interval) -> bool {
        return self.start < other.start && self.end < other.end
    }

    pub fn isPrev(&self,other:&Interval,step:usize) -> bool {
        return self.start < other.end-step
    }

    pub fn prev(&self,other:&Interval) -> Option<Interval>
    {
        // Compute the intresction of two interval x and y, where x is prev of y x.prev(y)
        let start = max(self.start, max(other.start-1, 0));
        let end = min(self.end, max(other.end-1,0));
        if start > 0 && end > 0 && end >= start {
            return Some(Interval{start,end});
        }
        return None;
    }


    pub fn overlap(&self,other:&Interval) -> bool {

        let flag=
            self.start <= other.end && self.end >= other.start;
        flag
    }

    pub fn intersect(&self, other:&Interval) -> Interval{
        let start = max(self.start,other.start);
        let end = min (self.end, other.end);
        return Interval{start,end}
    }


    // pub fn prev(&self) -> Interval{
    //     let min = self.start;
    //     return Interval{start, end:end-1}
    // }


}