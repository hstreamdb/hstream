HStream-SQL
===========


## File Structure

### etc: meta files, for reference only

- `SQL.cf`: BNFC grammar definition

### gen-sql: generated source files

- `HStream/SQL/Lex.x`: BNFC generated alex file
- `HStream/SQL/Par.y`: BNFC generated happy file
- `HStream/SQL/Lex.hs`: alex generated lexer module
- `HStream/SQL/Par.hs`: happy generated parser module
- `HStream/SQL/ErrM.hs`: BNFC generated module, ignore it
- `HStream/SQL/Print.hs`: BNFC generated pretty-print module
- `HStream/SQL/Abs.hs`: BNFC generated abstract syntax tree

### src: source files

- `HStream/SQL/Preprocess.hs`: user input preprocessing module
- `HStream/SQL/Internal/Validate.hs`: AST validation module
- `HStream/SQL/AST.hs`: Refined-AST definition and AST to Refined-AST transformation
- `HStream/SQL/Parse.hs`: user input to AST/Refined-AST functions
- `HStream/SQL/Codegen.hs`: Refined-AST to task AST transformation
- `HStream/SQL/Exception.hs`: SQL exception definition
- `HStream/SQL/Extra.hs`: some assistant functions (mostly used in `Validate.hs`)
