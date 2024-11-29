#!/usr/bin/env python3
"""
extract_signatures.py

A script to extract digital signatures from PDF and .p7m files.

Usage:
    python extract_signatures.py /path/to/your/file.pdf
    python extract_signatures.py /path/to/your/file.p7m
"""

import argparse
import json
import os
from typing import List, Union

import chilkat2

def extract_pdf_signatures(file_path: str) -> Union[List[str], str]:
    """
    Extract digital signatures from a PDF file using the Chilkat library.

    Args:
        file_path (str): Path to the PDF file.

    Returns:
        Union[List[str], str]: List of extracted signature information if successful,
                               or an error message string.
    """
    try:
        # Initialize the Chilkat Pdf object
        pdf = chilkat2.Pdf()

        # Load the PDF file
        if not pdf.LoadFile(file_path):
            return f"Failed to load PDF file: {pdf.LastErrorText}"

        # Get the number of signatures in the PDF
        num_signatures = pdf.NumSignatures
        if num_signatures == 0:
            return "No digital signatures found in the PDF."

        signatures = []
        for i in range(num_signatures):
            sig_info = chilkat2.JsonObject()
            sig_info.EmitCompact = False

            # Verify the signature and retrieve information
            validated = pdf.VerifySignature(i, sig_info)
            sig_details = {
                "SignatureIndex": i,
                "Validated": validated,
                "Details": json.loads(sig_info.Emit())
            }
            
            if "/Name" in sig_details["Details"]["signatureDictionary"]:
                name_hex = sig_details["Details"]["signatureDictionary"]["/Name"]
                try:
                    # Remove "<" and ">", then decode using UTF-16
                    if name_hex.startswith("<") and name_hex.endswith(">"):
                        name_hex = name_hex[1:-1]  # Strip "<" and ">"
                        sig_details["Details"]["signatureDictionary"]["/Name"] = bytes.fromhex(name_hex).decode('utf-16')
                except Exception as e:
                    sig_details["Details"]["signatureDictionary"]["/Name"] = f"Failed to decode: {e}"
            
            signatures.append(json.dumps(sig_details, indent=4))

        return signatures

    except AttributeError as e:
        return f"Chilkat PDF handling error: {str(e)}"


def extract_p7m_signatures(file_path: str) -> Union[List[str], str]:
    """
    Extract digital signatures from a .p7m file using the Chilkat2 library.

    Args:
        file_path (str): Path to the .p7m file.

    Returns:
        Union[List[str], str]: List of extracted signature information if successful,
                               or an error message string.
    """
    try:
        # Initialize the Chilkat2 Crypt2 object
        crypt = chilkat2.Crypt2()

        # Unlock the Chilkat component
        if not crypt.UnlockComponent("YourUnlockCode"):
            return f"Chilkat unlock failed: {crypt.LastErrorText}"

        # Verify the .p7m file and extract the original content
        output_file = "extracted_content"
        if not crypt.VerifyP7M(file_path, output_file):
            return f"Failed to verify .p7m file: {crypt.LastErrorText}"

        # Retrieve signer certificates
        num_signers = crypt.NumSignerCerts
        if num_signers == 0:
            return "No signers found in the .p7m file."

        signatures = []
        for i in range(num_signers):
            cert = crypt.GetSignerCert(i)
            if cert is not None:
                cert_info = {
                    "AuthorityKeyId": cert.AuthorityKeyId,
                    "CertVersion": cert.CertVersion,
                    "Expired": cert.Expired,
                    "ForClientAuthentication": cert.ForClientAuthentication,
                    "ForCodeSigning": cert.ForCodeSigning,
                    "ForSecureEmail": cert.ForSecureEmail,
                    "ForServerAuthentication": cert.ForServerAuthentication,
                    "ForTimeStamping": cert.ForTimeStamping,
                    "IssuerC": cert.IssuerC,
                    "IssuerCN": cert.IssuerCN,
                    "IssuerDN": cert.IssuerDN,
                    "IssuerO": cert.IssuerO,
                    "IssuerOU": cert.IssuerOU,
                    "IssuerS": cert.IssuerS,
                    "Revoked": cert.Revoked,
                    "SelfSigned": cert.SelfSigned,
                    "SerialNumber": cert.SerialNumber,
                    "Sha1Thumbprint": cert.Sha1Thumbprint,
                    "SubjectC": cert.SubjectC,
                    "SubjectCN": cert.SubjectCN,
                    "SubjectDN": cert.SubjectDN,
                    "SubjectO": cert.SubjectO,
                    "SubjectOU": cert.SubjectOU,
                    "SubjectS": cert.SubjectS,
                    "TrustedRoot": cert.TrustedRoot,
                    "ValidFromStr": cert.ValidFromStr,
                    "ValidToStr": cert.ValidToStr,
                }
                signatures.append(json.dumps(cert_info, indent=4))
            else:
                signatures.append(f"Failed to retrieve signer certificate at index {i}.")

        # Clean up the extracted file
        if os.path.exists(output_file):
            os.remove(output_file)

        return signatures

    except AttributeError as e:
        return f"Chilkat2 handling error: {str(e)}"


def main():
    parser = argparse.ArgumentParser(
        description='Extract digital signatures from PDF and .p7m files.'
    )
    parser.add_argument(
        'file_path',
        type=str,
        help='Path to the PDF or .p7m file from which to extract signatures.'
    )

    args = parser.parse_args()

    if not os.path.isfile(args.file_path):
        print(f"Error: File '{args.file_path}' not found.")
        return

    file_extension = os.path.splitext(args.file_path)[1].lower()

    if file_extension == '.pdf':
        result = extract_pdf_signatures(args.file_path)
    elif file_extension == '.p7m':
        result = extract_p7m_signatures(args.file_path)
    else:
        print("Unsupported file type. Please provide a PDF or .p7m file.")
        return

    if isinstance(result, list):
        if result:
            print("Extracted Signature Information:")
            for idx, info in enumerate(result, start=1):
                print(f"\nSignature {idx}:\n{info}")
        else:
            print("No digital signatures found in the file.")
    else:
        print(result)


if __name__ == '__main__':
    main()