use crate::interval::Interval;
use std::fs::File;
use std::path::Path;
use std::io::{BufReader, BufRead};
use itertools::Itertools;
use std::str::FromStr;
use std::fmt;
use crate::label::Label;


#[derive(Clone,Copy, Debug)]
pub struct Edge{
    pub eid: usize,
    pub src: usize,
    pub dst: usize,
    pub label: Label,
    pub prop1: Label,
    pub time: Interval
}

impl  Edge{
    pub fn get_from_file(filename: &str) -> Vec<Edge>
    {
        let mut edges: Vec<Edge> = vec![];
        let mut data_line = match File::open(&Path::new(&filename)) {
            Ok(file) => BufReader::new(file).lines(),
            Err(why) => panic!("EXCEPTION: couldn't open {}: {}",
                               Path::new(&filename).display(),
                               why.to_string(),
            )
        };
        // read the data
        for (_, line) in data_line.by_ref().enumerate() {
            let good_line = line.ok().expect("EXCEPTION: read error");
            if !good_line.starts_with('#') && good_line.len() > 0 {
                let mut elements = good_line[..].split(",");
                let nid: usize = elements.next().unwrap().parse().ok().expect("malformed src");
                let src: usize = elements.next().unwrap().parse().ok().expect("malformed src");
                let dst: usize = elements.next().unwrap().parse().ok().expect("malformed src");
                let label: Label = elements.next().unwrap().parse().ok().expect("malformed src");
                let prop1: Label = elements.next().unwrap().parse().ok().expect("malformed src");
                let start: usize = elements.next().unwrap().parse().ok().expect("malformed src");
                let end: usize = elements.next().unwrap().parse().ok().expect("malformed src");
                let n = Edge {
                    eid: nid,
                    src,
                    dst,
                    label,
                    prop1,
                    time: Interval {start,end},
                };
                edges.push(n)
            }
        }
        return edges;
    }

    pub fn getByDate(mut inp:Vec<Edge>, date:Interval)-> impl Iterator<Item=Edge > + 'static {
        let out = inp.into_iter().filter(move |m| m.time.overlap(&date))
            .map(move | m| Edge{
                eid: m.eid,
                src: m.src,
                dst: m.dst,
                label: m.label,
                prop1: m.prop1,
                time: m.time.intersect(&date)
            });

        return out ;
    }
}
