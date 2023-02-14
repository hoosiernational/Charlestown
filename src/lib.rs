use std::collections::HashMap;

/**!

Charlestown is a simple RFC 4180-compliant reader and writer for CSVs

*/

enum BytestreamReaderResult {
    LastOfLine(Vec<u8>),
    NonTerminalCell(Vec<u8>),
}

struct CSVReader {
    bytes: Vec<u8>,
    ptr: usize,
    len: usize,
}

impl CSVReader {
    fn eof(&self) -> bool {
        self.ptr == self.len
    }

    fn pop(&mut self) -> Result<u8, ()> {
        if self.eof() {
            Err(())
        } else {
            let output = self.bytes[self.ptr];
            self.ptr += 1;
            Ok(output)
        }
    }

    fn peek(&self) -> Result<u8, ()> {
        if self.eof() {
            Err(())
        } else {
            Ok(self.bytes[self.ptr])
        }
    }

    fn from_vec(input: Vec<u8>) -> Self {
        let len = input.len();
        Self {
            bytes: input,
            ptr: 0,
            len: len,
        }
    }

    fn to_bytestream_reader_results(&mut self) -> Vec<BytestreamReaderResult> {
        let mut output = Vec::<BytestreamReaderResult>::new();
        let mut current_cell = Vec::<u8>::new();
        let mut in_quotes = false;
        while !self.eof() {
            match self.pop().unwrap() {
                0x22 => {
                    if in_quotes {
                        if self.peek() == Ok(0x22) {
                            current_cell.push(0x22);
                            self.pop().unwrap();
                        } else {
                            in_quotes = false;
                        }
                    } else {
                        in_quotes = true;
                    }
                }
                0x2C => {
                    if in_quotes {
                        current_cell.push(0x2C);
                    } else {
                        output.push(BytestreamReaderResult::NonTerminalCell(
                            current_cell.clone(),
                        ));
                        current_cell.clear();
                    }
                }
                0x0D => {
                    if in_quotes {
                        current_cell.push(0x0D);
                    } else if self.peek() == Ok(0x0A) {
                        self.pop().unwrap();
                        output.push(BytestreamReaderResult::LastOfLine(current_cell.clone()));
                        current_cell.clear();
                    } else {
                        current_cell.push(0x0D);
                    }
                }
                0x0A => {
                    if in_quotes {
                        current_cell.push(0x0A);
                    } else {
                        output.push(BytestreamReaderResult::LastOfLine(current_cell.clone()));
                        current_cell.clear();
                    }
                }
                r => {
                    current_cell.push(r);
                }
            }
        }
        if !current_cell.is_empty() {
            output.push(BytestreamReaderResult::LastOfLine(current_cell));
        }
        output
    }

    fn to_unheadered_csv_input_table_contents(&mut self) -> Vec<Vec<String>> {
        let bytestream_reader_results = self.to_bytestream_reader_results();
        let mut table: Vec<Vec<String>> = Vec::new();
        let mut current_row = Vec::<String>::new();
        for bsr_i in bytestream_reader_results {
            match bsr_i {
                BytestreamReaderResult::NonTerminalCell(ntc) => {
                    current_row.push(String::from_utf8(ntc).unwrap().trim().to_owned());
                }
                BytestreamReaderResult::LastOfLine(ntc) => {
                    current_row.push(String::from_utf8(ntc).unwrap().trim().to_owned());
                    table.push(current_row.clone());
                    current_row.clear();
                }
            }
        }
        table
    }
}

///A CSV Table with no header
#[derive(Clone)]
pub struct UnheaderedCSVTable(Vec<Vec<String>>);

impl UnheaderedCSVTable {
    ///Declares a new UnheaderedCSVTable based around an existing vec (table) of vecs (rows) of Strings (cells)
    pub fn from_rows(rows: Vec<Vec<String>>) -> Self {
        Self(rows)
    }

    ///Pushes a new row (vector of strings) to this table.
    pub fn push_row(&mut self, input: Vec<String>) {
        self.0.push(input);
    }

    ///Gets a row (as a result of a string vector)
    pub fn get_row(&self, row_index: usize) -> Result<Vec<String>, ()> {
        match self.0.get(row_index) {
            None => Err(()),
            Some(r) => Ok(r.clone()),
        }
    }

    ///Returns the number of rows in the table.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    ///Returns all members of a column as a vector of string results
    pub fn get_column(&self, input: usize) -> Vec<Result<String, ()>> {
        self.0
            .clone()
            .into_iter()
            .map(|x| match x.get(input) {
                None => Err(()),
                Some(r) => Ok(r.clone()),
            })
            .collect::<Vec<Result<String, ()>>>()
    }

    ///Returns the contents of an indexed cell as a string result
    pub fn get_cell(&self, row: usize, column: usize) -> Result<String, ()> {
        match self.get_row(row) {
            Err(()) => Err(()),
            Ok(row) => match row.get(column) {
                None => Err(()),
                Some(r) => Ok(r.clone()),
            },
        }
    }

    fn csv_sanitize(y: String) -> String {
        if y.contains("\"") || y.contains(",") || y.contains("\n") || y.contains("\r") {
            format!("\"{}\"", y.replace("\"", "\"\""))
        } else {
            y
        }
    }

    ///Turns this table into a String that can serve as the contents of a CSV file
    pub fn stringify(&self) -> String {
        self.0
            .clone()
            .into_iter()
            .map(|x| {
                x.into_iter()
                    .map(|y| Self::csv_sanitize(y))
                    .collect::<Vec<String>>()
                    .join(",")
            })
            .collect::<Vec<String>>()
            .join("\r\n")
    }

    ///Saves this table to a CSV file
    pub fn save(&self, path: String) -> Result<(), ()> {
        match std::fs::write(path, self.stringify()) {
            Ok(()) => Ok(()),
            Err(_) => Err(()),
        }
    }

    ///Turns a byte vector (like the contents of a CSV file) into an UnheaderedCSVTable instance
    pub fn from_byte_vector(input: Vec<u8>) -> Self {
        Self(CSVReader::from_vec(input).to_unheadered_csv_input_table_contents())
    }

    ///Turns a string (like the contents of a CSV file) into an UnheaderedCSVTable instance
    pub fn from_string(input: &str) -> UnheaderedCSVTable {
        Self(
            CSVReader::from_vec(input.as_bytes().to_vec()).to_unheadered_csv_input_table_contents(),
        )
    }

    ///Reads an UnheaderedCSVTable from a file. Note that this can be called on headered CSV
    ///files, but the first row will be assumed to contain a record.
    pub fn from_file_location(path: &str) -> Result<UnheaderedCSVTable, ()> {
        match std::fs::read_to_string(path) {
            Err(_) => Err(()),
            Ok(string) => Ok(Self::from_byte_vector(string.as_bytes().to_vec())),
        }
    }
}

///A CSV table with a header (thus each column is named)
#[derive(Clone)]
pub struct HeaderedCSVTable {
    columns: HashMap<String, usize>,
    rows: Vec<Vec<String>>,
}

impl HeaderedCSVTable {
    ///Creates a HeaderedCSVTable from an UnheaderedCSVTable by assuming the first column is a header.
    ///Note that this may add extra cells and columns to balance the table.
    pub fn from_unheadered_csv_table(input: UnheaderedCSVTable) -> Self {
        let mut top_row = input.get_row(0).unwrap_or(Vec::new());
        let mut max_rows_size = top_row.len();
        for i in 1..input.len() {
            if input.get_row(i).unwrap().len() > max_rows_size {
                max_rows_size = input.get_row(i).unwrap().len();
            }
        }
        while top_row.len() < max_rows_size {
            top_row.push(format!("{}", top_row.len()));
        }
        let mut rows = Vec::<Vec<String>>::new();
        for i in 1..input.len() {
            let mut this_row = input.get_row(i).unwrap();
            while this_row.len() < max_rows_size {
                this_row.push(String::new());
            }
            rows.push(this_row);
        }
        let mut columns = HashMap::<String, usize>::new();
        let top_row_string_vector = top_row;
        for i in 0..top_row_string_vector.len() {
            columns.insert(top_row_string_vector[i].clone(), i);
        }
        Self { columns, rows }
    }

    ///Gets an indexed row as a vector of strings. Note that index 0 would refer to the second
    ///row of the CSV file, as the first row is now assumed to be a header
    pub fn get_unheadered_row(&self, input: usize) -> Result<Vec<String>, ()> {
        match self.rows.get(input) {
            None => Err(()),
            Some(r) => Ok(r.clone()),
        }
    }

    ///Gets an indexed row as a HashMap, where cells are accessed by their column header value.
    ///Note that index 0 would refer to the second
    ///row of the CSV file, as the first row is now assumed to be a header
    pub fn get_headered_row(&self, input: usize) -> Result<HashMap<String, String>, ()> {
        match self.get_unheadered_row(input) {
            Err(()) => Err(()),
            Ok(unheadered_row) => Ok(self
                .columns
                .clone()
                .into_iter()
                .map(|(x, y)| (x, unheadered_row.get(y).unwrap().clone()))
                .collect::<HashMap<String, String>>()),
        }
    }

    ///Gets the number of rows in the table
    pub fn number_of_rows(&self) -> usize {
        self.rows.len()
    }

    ///Gets the number of columns in the table. Note that this method does not exist for
    ///UnheaderedCSVTable, as unheadered tables are not guaranteed to be balanced
    pub fn number_of_columns(&self) -> usize {
        self.columns.len()
    }

    ///Gets the cells within a column as a vector of String results. If a column is called which
    ///does not exist, an array of Err(()) values will be returned with the same magnitude as
    ///the body of the table.
    pub fn get_column(&self, input: &str) -> Vec<Result<String, ()>> {
        let input_index = self.columns.get(input);
        match input_index {
            None => {
                let mut v = Vec::<Result<String, ()>>::new();
                for _ in 0..self.rows.len() {
                    v.push(Err(()));
                }
                v
            }
            Some(input_index) => self
                .rows
                .clone()
                .into_iter()
                .map(|x| match x.get(*input_index) {
                    None => Err(()),
                    Some(r) => Ok(r.clone()),
                })
                .collect::<Vec<Result<String, ()>>>(),
        }
    }

    ///Returns the value of a cell as a string result
    pub fn get_cell(&self, row: usize, column: &str) -> Result<String, ()> {
        match self.columns.get(column) {
            None => Err(()),
            Some(column_index) => match self.get_unheadered_row(row) {
                Err(()) => Err(()),
                Ok(row) => match row.get(*column_index) {
                    None => Err(()),
                    Some(r) => Ok(r.clone()),
                },
            },
        }
    }

    ///Turns this table into an unheadered table. Note that this is not the exact inverse of
    ///from_unheadered_csv_input_table, as from_unheadered_csv_input_table balances the table
    pub fn to_unheadered_csv_input_table(&self) -> UnheaderedCSVTable {
        let mut inverted_columns =
            HashMap::<usize, String>::with_capacity(self.number_of_columns());
        self.columns.clone().into_iter().for_each(|(x, y)| {
            inverted_columns.insert(y, x);
        });
        let mut first_row = Vec::<String>::with_capacity(self.number_of_columns());
        for i in 0..self.number_of_columns() {
            first_row.push(inverted_columns.get(&i).unwrap().clone());
        }
        let mut all_rows = vec![first_row];
        for row in self.rows.clone() {
            all_rows.push(row.clone());
        }
        UnheaderedCSVTable::from_rows(all_rows)
    }

    ///Turns this table into a string that can serve as the contents of a CSV file
    pub fn stringify(&self) -> String {
        self.to_unheadered_csv_input_table().stringify()
    }

    ///Saves this to a CSV file
    pub fn save(&self, path: String) -> Result<(), ()> {
        match std::fs::write(path, self.stringify()) {
            Ok(()) => Ok(()),
            Err(_) => Err(()),
        }
    }

    ///Turns a byte vector (like the contents of a CSV file) into a HeaderedCSVTable instance
    pub fn from_byte_vector(input: Vec<u8>) -> Self {
        Self::from_unheadered_csv_table(UnheaderedCSVTable::from_byte_vector(input))
    }

    ///Turns a string (like the contents of a CSV file) into a HeaderedCSVTable instance
    pub fn from_string(input: &str) -> Self {
        Self::from_unheadered_csv_table(UnheaderedCSVTable::from_string(input))
    }

    ///Reads an HeaderedCSV from a file. The first row will be assumed to contain the header.
    pub fn from_file_location(path: &str) -> Result<Self, ()> {
        match std::fs::read_to_string(path) {
            Err(_) => Err(()),
            Ok(string) => Ok(Self::from_byte_vector(string.as_bytes().to_vec())),
        }
    }
}
