from pathlib import Path
p = Path('data/large_text.txt')
sample = ('This is a synthetic large text file generated for performance testing.\n'
          'It contains repeated paragraphs to simulate large documents used by the pipeline.\n\n'
          'Paragraph: Support operations logs, product inspection results, customer communications, shipment delay explanations, refund eligibility notes, incident report summaries, inventory sync messages, audit logs, diagnostics, service updates, and operational guidance.\n\n')
count = 500
with p.open('w', encoding='utf-8') as f:
    for i in range(count):
        f.write(f"--PARA {i+1}--\n")
        f.write(sample)
print('Wrote', p.stat().st_size, 'bytes to', p)
