use std::str::FromStr;
use std::fmt;

#[derive(Clone,Copy,Debug)]
pub enum Label {
    meets,
    rides,
    cohab,
    cafe,
    park,
    person,
    bus,
    Ann,
    Bob,
    Mia,
    Eve,
    Zoe,
    Q48,
    B101,
    Queen,
    Bronx,
    low,
    high,
    neg,
    pos,
    default,


}
impl FromStr for Label {

    type Err = ();

    fn from_str(input: &str) -> Result<Label, Self::Err> {
        match input {
            "meets"  => Ok(Label::meets),
            "rides"  => Ok(Label::rides),
            "cohab"  => Ok(Label::cohab),

            "person" =>  Ok(Label::person),
            "bus" =>  Ok(Label::bus),
            "cafe"  => Ok(Label::cafe),
            "park"  => Ok(Label::park),
            "Ann" =>  Ok(Label::Ann),
            "Bob" =>  Ok(Label::Bob),
            "Mia" =>  Ok(Label::Mia),
            "Eve" =>  Ok(Label::Eve),
            "Zoe" =>  Ok(Label::Zoe),
            "Q48" =>  Ok(Label::Q48),
            "B101" =>  Ok(Label::B101),
            "Queen" =>  Ok(Label::Queen),
            "Bronx" =>  Ok(Label::Bronx),
            "low" =>  Ok(Label::low),
            "high" =>  Ok(Label::high),
            "neg" =>  Ok(Label::neg),
            "pos" =>  Ok(Label::pos),
            "" => Ok(Label::default),
            _      => Err(()),
        }
    }
}

// impl fmt::Debug for Label {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         match self{
//             Label::default =>   write!(f, ""),
//             _ => write!(f, "{:?}", self)
//
//
//         }
//     }
// }
