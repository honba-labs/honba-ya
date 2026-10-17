use anyhow::Result;

pub fn show(year: i32) -> Result<()> {
    // TODO: use honba-market NseCalendar to list trading days for `year`.
    println!("Trading calendar for year {year}");
    Ok(())
}
