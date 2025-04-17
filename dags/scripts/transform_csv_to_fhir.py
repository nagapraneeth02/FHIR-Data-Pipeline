import pandas as pd
import os
import re
from datetime import datetime, date
from fhir.resources.patient import Patient
from fhir.resources.encounter import Encounter, EncounterDiagnosis
from fhir.resources.condition import Condition
from fhir.resources.procedure import Procedure
from fhir.resources.claim import Claim
from fhir.resources.reference import Reference
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.coding import Coding
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.period import Period
from fhir.resources.money import Money
from fhir.resources.bundle import Bundle, BundleEntry

def clean_id(raw_id):
    return re.sub(r'[^A-Za-z0-9\-.]', '-', str(raw_id))

def csv_to_fhir(input_path, output_path):
    df = pd.read_csv(input_path, encoding='latin1', low_memory=False)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    bundle = Bundle.construct()
    bundle.type = "collection"
    bundle.entry = []

    for _, row in df.iterrows():
        patient = Patient.construct(
            id=str(row['MEMBER_KEY']),
            identifier=[{"system": "http://example.com/member_id", "value": str(row['MEMBER_ID'])}],
            gender=row['MEM_GENDER'].lower() if pd.notna(row['MEM_GENDER']) else None
        )
        if pd.notna(row['MEM_DOB']):
            patient.birthDate = datetime.strptime(row['MEM_DOB'], '%m/%d/%Y').date().isoformat()
        if pd.notna(row['MEM_ZIP']):
            patient.address = [{"postalCode": str(row['MEM_ZIP'])}]
        bundle.entry.append(BundleEntry(resource=patient))

        encounter_id = clean_id(f"enc-{row['CLAIM_ID']}")
        encounter = Encounter.construct(
            id=encounter_id,
            status="finished",
            subject=Reference(reference=f"Patient/{row['MEMBER_KEY']}")
        )
        if pd.notna(row.get('Inpatient/ outpatient')):
            encounter.type = [CodeableConcept.construct(
                coding=[Coding.construct(
                    system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
                    code="IMP" if row['Inpatient/ outpatient'] == "Inpatient" else "AMB"
                )]
            )]
        if pd.notna(row['AdmitDate']) and pd.notna(row['DischargeDate']):
            encounter.actualPeriod = Period.construct(
                start=datetime.strptime(row['AdmitDate'], '%m/%d/%Y').isoformat(),
                end=datetime.strptime(row['DischargeDate'], '%m/%d/%Y').isoformat()
            )
        if pd.notna(row.get('DRG_CODE')):
            diagnosis = EncounterDiagnosis.construct(
                condition=[CodeableReference.construct(reference=Reference.construct(reference=f"Condition/cond-{row['CLAIM_ID']}-primary"))],
                use=[CodeableConcept.construct(
                    coding=[Coding.construct(
                        system="http://hl7.org/fhir/diagnosis-role",
                        code="billing"
                    )]
                )]
            )
            encounter.diagnosis = [diagnosis]
        bundle.entry.append(BundleEntry(resource=encounter))

        for i in range(1, 5):
            proc_code = row.get(f'proc{i}_code')
            proc_desc = row.get(f'proc{i}_desc')
            if pd.notna(proc_code):
                procedure = Procedure.construct(
                    id=clean_id(f"proc-{row['CLAIM_ID']}-{i}"),
                    status="completed",
                    subject=Reference.construct(reference=f"Patient/{row['MEMBER_KEY']}"),
                    encounter=Reference.construct(reference=f"Encounter/{encounter_id}"),
                    code=CodeableConcept.construct(
                        coding=[Coding.construct(
                            system="http://www.ama-assn.org/go/cpt",
                            code=str(proc_code),
                            display=proc_desc if pd.notna(proc_desc) else None
                        )]
                    )
                )
                bundle.entry.append(BundleEntry(resource=procedure))

        claim = Claim.construct(
            id=str(row['CLAIM_ID']),
            status="active",
            use="claim",
            created=date.today().isoformat(),
            patient=Reference.construct(reference=f"Patient/{row['MEMBER_KEY']}"),
            encounter=[Reference.construct(reference=f"Encounter/{encounter_id}")],
            type=CodeableConcept.construct(
                coding=[Coding.construct(
                    system="http://terminology.hl7.org/CodeSystem/claim-type",
                    code="institutional"
                )]
            )
        )
        if pd.notna(row['AMT_BILLED']):
            claim.total = Money.construct(value=float(row['AMT_BILLED']), currency="USD")
        if pd.notna(row['PayerType']):
            claim.insurer = {"display": row['PayerType']}
        bundle.entry.append(BundleEntry(resource=claim))

    with open(output_path, 'w') as f:
        f.write(bundle.json(indent=2))

if __name__ == "__main__":
    input_file = "/opt/airflow/data/input/healthcare_data.csv"
    output_file = "/opt/airflow/data/output/fhir_output.json"
    csv_to_fhir(input_file, output_file)
