use crate::interval::Interval;
use std::fs::File;
use std::path::Path;
use std::io::{BufReader, BufRead};
use itertools::Itertools;
use itertools::__std_iter::{Filter, Map};
use crate::label::Label;

#[derive(Clone,Copy,Debug)]
pub struct Node{
    pub nid: usize,
    pub label: Label,
    pub prop1: Label,
    pub prop2: Label,
    pub prop3: Label,
    pub time: Interval
}

impl  Node{
    pub fn get_from_file(filename: &str) -> Vec<Node>
    {
        let mut nodes: Vec<Node> = vec![];
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
                let label: Label = elements.next().unwrap().parse().ok().expect("malformed src");
                let prop1: Label = elements.next().unwrap().parse().ok().expect("malformed src");
                let prop2: Label = elements.next().unwrap().parse().ok().expect("malformed src");
                let prop3: Label = elements.next().unwrap().parse().ok().expect("malformed src");
                let start: usize = elements.next().unwrap().parse().ok().expect("malformed src");
                let end: usize = elements.next().unwrap().parse().ok().expect("malformed src");
                let n = Node {
                    nid,
                    label,
                    prop1,
                    prop2,
                    prop3,
                    time: Interval {start,end},
                };
                println!("{:?}",&n);
                nodes.push(n)
            }
        }
        return nodes;
    }

    pub fn getByDate(mut inp:Vec<Node>, date:Interval)-> impl Iterator<Item=Node> + 'static {

        let out = inp.into_iter().filter(move | m| m.time.overlap( &date))
            .map(move | m| Node{
                nid: m.nid,
                label: m.label,
                prop1: m.prop1,
                prop2: m.prop2,
                prop3: m.prop3,
                time: m.time.intersect(&date)
            });

        return out ;
    }

}
