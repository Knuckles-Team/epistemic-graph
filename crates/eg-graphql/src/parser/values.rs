use super::cursor::Cursor;
use super::error::GqlError;
use super::lexer::Tok;
use super::GqlValue;

pub(super) struct ValueParser<'cursor, 'src> {
    cursor: &'cursor mut Cursor<'src>,
}

impl<'cursor, 'src> ValueParser<'cursor, 'src> {
    pub(super) fn new(cursor: &'cursor mut Cursor<'src>) -> Self {
        Self { cursor }
    }

    pub(super) fn parse_args(&mut self) -> Result<Vec<(String, GqlValue)>, GqlError> {
        self.cursor.expect(&Tok::LParen, "`(`")?;
        let mut args = Vec::new();
        while !self.cursor.peek_is(&Tok::RParen) && self.cursor.peek().is_some() {
            let name = self.cursor.expect_name("an argument name")?;
            self.cursor
                .expect(&Tok::Colon, "`:` after the argument name")?;
            let value = self.parse_value()?;
            args.push((name, value));
            // commas are optional (lexed as whitespace), but tolerate a stray one.
            let _ = self.cursor.eat(&Tok::Comma);
        }
        self.cursor
            .expect(&Tok::RParen, "`)` to close the arguments")?;
        Ok(args)
    }

    pub(super) fn parse_value(&mut self) -> Result<GqlValue, GqlError> {
        match self.cursor.peek().cloned() {
            Some(Tok::Int(n)) => {
                self.cursor.bump();
                Ok(GqlValue::Int(n))
            }
            Some(Tok::Float(f)) => {
                self.cursor.bump();
                Ok(GqlValue::Float(f))
            }
            Some(Tok::Str(s)) => {
                self.cursor.bump();
                Ok(GqlValue::Str(s))
            }
            Some(Tok::Name(n)) if n == "true" || n == "false" => {
                self.cursor.bump();
                Ok(GqlValue::Bool(n == "true"))
            }
            Some(Tok::Name(n)) if n == "null" => {
                self.cursor.bump();
                Ok(GqlValue::Null)
            }
            // A `$name` variable reference (CONCEPT:EG-KG.query.fragments-variables-directives).
            Some(Tok::Dollar) => {
                self.cursor.bump();
                let name = self.cursor.expect_name("a variable name after `$`")?;
                Ok(GqlValue::Var(name))
            }
            // An input object `{ field: value, … }` (CONCEPT:EG-KG.query.mutation — a mutation `props`).
            Some(Tok::LBrace) => self.parse_object_value(),
            // A list `[ value, … ]`.
            Some(Tok::LBracket) => self.parse_list_value(),
            _ => Err(self.cursor.err(
                "expected an argument value (int, float, string, bool, null, \
                 variable, object, or list)",
            )),
        }
    }

    /// Parse an input-object value `{ field: value, … }` (commas optional).
    pub(super) fn parse_object_value(&mut self) -> Result<GqlValue, GqlError> {
        self.cursor
            .expect(&Tok::LBrace, "`{` to open an input object")?;
        let fields = parse_comma_separated(
            self.cursor,
            &Tok::RBrace,
            "`}` to close the input object",
            |parser| {
                let name = parser.expect_name("an input-object field name")?;
                parser.expect(&Tok::Colon, "`:` after the input-object field name")?;
                let value = ValueParser::new(parser).parse_value()?;
                Ok((name, value))
            },
        )?;
        Ok(GqlValue::Object(fields))
    }

    /// Parse a list value `[ value, … ]` (commas optional).
    pub(super) fn parse_list_value(&mut self) -> Result<GqlValue, GqlError> {
        self.cursor.expect(&Tok::LBracket, "`[` to open a list")?;
        let items = parse_comma_separated(
            self.cursor,
            &Tok::RBracket,
            "`]` to close the list",
            |cursor| ValueParser::new(cursor).parse_value(),
        )?;
        Ok(GqlValue::List(items))
    }
}

fn parse_comma_separated<'src, T>(
    cursor: &mut Cursor<'src>,
    close: &Tok,
    close_message: &str,
    mut parse_item: impl FnMut(&mut Cursor<'src>) -> Result<T, GqlError>,
) -> Result<Vec<T>, GqlError> {
    let mut items = Vec::new();
    while !cursor.peek_is(close) && cursor.peek().is_some() {
        items.push(parse_item(cursor)?);
        let _ = cursor.eat(&Tok::Comma);
    }
    cursor.expect(close, close_message)?;
    Ok(items)
}
