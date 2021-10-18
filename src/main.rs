extern crate tpath;
use itertools::Itertools;

use tpath::interval;
use tpath::interval::Interval;
use tpath::node::Node;
use tpath::edge::{Edge};
use std::collections::HashMap;
use std::ops::Index;
use std::ptr::null;
use tpath::join::hash_join;
use tpath::label::Label;


fn main() {
    let i = Interval {start:1, end : 10};
    println!("{:?}", i.toPoints());

    let n = Node::get_from_file("/Users/amir/Documents/Projects/rust/tpath/data/simp/node.csv");
    println!("{:?}", n);

    let e = Edge::get_from_file("/Users/amir/Documents/Projects/rust/tpath/data/simp/edge.csv");
    println!("{:?}", e);


    // MATCH (x: Person) ON contact_tracing
    let q1= n.iter().filter(|p| matches!(p.label,Label::person))
        .flat_map(|p|(p.time.zip(p.nid)))
        .collect_vec();
    println!("q1(x,t): {:?}", q1);

    // MATCH (x: Person { risk = 'low '}) ON contact_tracing
    let q2= n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low))
        .flat_map(|p|(p.time.zip(p.nid))).collect_vec();
    println!("q2(x,t): {:?}", q2);


    // MATCH (x: Person { risk = 'low ' AND time = '1'}) ON contact_tracing
    let q3= n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low))
        .flat_map(|p|(p.time.zip(p.nid)))
        .filter(|(pid,time)| *time == 1)
        .collect_vec();
    println!("q3(x,t): {:?}", q3);

    // MATCH (x: Person { risk = 'low ' AND time = '1'}) ON contact_tracing
    let q4= n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low))
        .flat_map(|p|(p.time.zip(p.nid)))
        .filter(|(pid,time)| *time < 10)
        .collect_vec();
    println!("q4(x,t): {:?}", q4);



    /* MATCH (x: Person { risk = 'low '}) -
        [z: meets ]->(y: Person { risk = 'high '})
        ON contact_tracing */

    let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low)).map(|p| (p.nid,p.clone())).collect_vec();
    let y = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid,p.clone())).collect_vec();
    let mut zp = e.iter().filter(|e| matches!(e.label,Label::meets)).map(|e| (e.src, e.clone())).collect_vec();
    zp = hash_join(&zp,&x).into_iter()
        .filter_map(|(e,_,n)| match e.time.overlap(&n.time) {
            true => Some(Edge{
                eid: e.eid,
                src: e.src,
                dst: e.dst,
                label: e.label,
                prop1: e.prop1,
                time: e.time.intersect(&n.time)
            }),
            false => None
        }).map(|e| (e.dst,e))
        .collect_vec();
    let q5 = hash_join(&zp,&y).into_iter()
        .filter_map(|(e,_,n)| match e.time.overlap(&n.time) {
            true => Some(Edge{
                eid: e.eid,
                src: e.src,
                dst: e.dst,
                label: e.label,
                prop1: e.prop1,
                time: e.time.intersect(&n.time)
            }),
            false => None
        })
        .flat_map(|e|(e.time.zip_vec(&vec![e.src,e.eid,e.dst])))
        //.map(|e|(e.time.zip(e.src),(e.time.zip(e.eid)),(e.time.zip(e.dst))))
        .collect_vec();
     println!("q5[(x,t),(z,t),(t,t)]: {:?}", q5);


    // MATCH (x: Person { test = 'pos '}) - / PREV /-(y: Person ) ON contact_tracing ????
    let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos))/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
    let q6 = hash_join(&n.iter().map(|p|(p.nid,p.time.clone())).collect_vec(),&x.map(|x| (x.nid,x.time.clone())).collect_vec())
        .into_iter()
        .filter(|(yt,x,xt)| yt.isBefore(xt))
        .filter_map(|(yt,x,xt)|
            match yt.prev(&xt){
                None => {None}
                Some(yt) =>  Some(( xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt,yt)| *xt-1 == *yt  ).map(move |(xt,yt)|((x,xt),(x,yt)))))
            })
        .flat_map(|x| x)
        .collect_vec();
    println!("q6: {:?}", q6);


    //  MATCH (x: Person { test = 'pos '}) -
    //      / PREV /-() -[: rides ]->(z:Bus )
    //  ON contact_tracing

    let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid,p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
    let z = e.iter().filter(|e| matches!(e.label,Label::rides)).map(|e| (e.src, e.time.clone())).collect_vec();
    let q7 = hash_join(&z,&x).into_iter()
        .filter_map(|(zt,x,xt)|
            match zt.prev(&xt) {
                None => None,
                Some(zt) =>  Some(( xt.toPoints().into_iter().cartesian_product(zt.toPoints()).map(move |(xt,zt)|((x,xt),(x,zt)))))
                })
        //.map (|(zt,x,xt)| (x, zt.toPoints().into_iter().cartesian_product(xt.toPoints()).filter(|(zt,xt)|*zt == xt-1).collect_vec()))
        .flat_map(|x| x)
        .collect_vec();
    println!("q7: {:?}", q7);


    //  MATCH (x: Person { test = 'pos '}) -
    //      / PREV* /-() -[: rides ]->(z:Bus )
    //  ON contact_tracing

    let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid,p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
    let z = e.iter().filter(|e| matches!(e.label,Label::rides)).map(|e| (e.src, e.time.clone())).collect_vec();
    let q8 = hash_join(&z,&x).into_iter()
        .filter(|(zt,x,xt)| zt.isBefore(xt))
        .map (|(zt,x,xt)| (xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(zt,xt)|*zt-*xt> 0).map(move |(xt,zt)|((x,xt),(x,zt)))))
        .flat_map(|x| x)
        .collect_vec();
    println!("q8: {:?}", q8);


    // // MATCH (x: Person { risk = 'high '}) -
    // //     / FWD /: meets / FWD / NEXT */ -({ test = 'pos '})
    // //     ON contact_tracing
    //
    // let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid,p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
    // let y = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid,p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
    // let zy  = e.iter().filter(|e| e.src == x)
    // println!("q9: {:?}", x);


}
